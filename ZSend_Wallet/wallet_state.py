from __future__ import annotations

from .helpers import tx_status_code
from .wallet_cache import WalletCache, zat_to_float


INACTIVE_TX_STATUSES = {"failed", "expired", "conflicted", "reorged", "stale"}


def wallet_identity_candidate(data: dict) -> str:
    addresses = [
        str(addr or "").strip()
        for addr in list((data or {}).get("t_addrs", []) or [])
        + list((data or {}).get("z_addrs", []) or [])
    ]
    return min({addr for addr in addresses if addr}, default="")


def derive_busy_addresses(
    data: dict,
    cache: WalletCache | None,
    *,
    pending_send: dict | None = None,
    active_opid: str = "",
) -> set[str]:
    busy: set[str] = set()
    live_txid_confirms: dict[str, int] = {}
    live_txid_status: dict[str, str] = {}
    for tx in data.get("txs", []) or []:
        try:
            confirms = int(tx.get("confirmations", 0) or 0)
        except Exception:
            confirms = 0
        txid = str(tx.get("txid", "") or "").strip()
        status = str(tx.get("status", "") or "").strip().lower()
        if txid:
            live_txid_confirms[txid] = confirms
            live_txid_status[txid] = status
        if status in INACTIVE_TX_STATUSES or confirms != 0:
            continue
        address = str(tx.get("address", "") or "").strip()
        if address:
            busy.add(address)
        for entry in tx.get("_entries", []) or []:
            entry_address = str(entry.get("address", "") or "").strip()
            if entry_address:
                busy.add(entry_address)

    pending_from = str((pending_send or {}).get("from", "") or "").strip()
    if pending_from and active_opid:
        busy.add(pending_from)
    if cache is None:
        return busy

    try:
        operations = cache.list_operations(limit=200)
    except Exception:
        return busy
    for op in operations:
        from_address = str(op.get("from_address", "") or "").strip()
        to_address = str(op.get("to_address", "") or "").strip()
        if not from_address:
            continue
        status = str(op.get("status", "") or "").strip().lower()
        txid = str(op.get("txid", "") or "").strip()
        if status in {"submitted", "queued", "executing"}:
            busy.add(from_address)
            if to_address:
                busy.add(to_address)
            continue
        if status != "success":
            continue
        if not txid:
            busy.add(from_address)
            continue
        if live_txid_status.get(txid, "") in INACTIVE_TX_STATUSES:
            continue
        confirmations = live_txid_confirms.get(txid)
        if confirmations is None:
            try:
                cached_entries = cache.get_transaction_entries(txid)
            except Exception:
                cached_entries = []
            cached_statuses = {
                str(row.get("status", "") or "").strip().lower()
                for row in cached_entries
            }
            if cached_statuses & INACTIVE_TX_STATUSES:
                continue
            cached_confirmations = [
                int(row.get("confirmations", 0) or 0)
                for row in cached_entries
            ]
            if cached_confirmations:
                confirmations = max(cached_confirmations)
        if confirmations is None or confirmations <= 0:
            busy.add(from_address)
            if to_address:
                busy.add(to_address)
    return busy


def success_operation_txids(cache: WalletCache | None) -> set[str]:
    if cache is None:
        return set()
    try:
        return {
            str(op.get("txid", "") or "").strip()
            for op in cache.list_operations(status="success", limit=200)
            if str(op.get("txid", "") or "").strip()
        }
    except Exception:
        return set()


def fast_status_txids(
    data: dict,
    cached_txs: list,
    cache: WalletCache | None,
    *,
    active_opid: str = "",
) -> list[str]:
    tracked: list[str] = []
    seen: set[str] = set()

    def add(value: str):
        txid = str(value or "").strip()
        if txid and txid not in seen:
            seen.add(txid)
            tracked.append(txid)

    if active_opid and cache is not None:
        try:
            for op in cache.list_operations(limit=50):
                if str(op.get("opid", "") or "") == active_opid:
                    add(str(op.get("txid", "") or ""))
                    break
        except Exception:
            pass

    for tx in list(cached_txs or []) + list((data or {}).get("txs", []) or []):
        txid = str(tx.get("txid", "") or "").strip()
        if not txid:
            continue
        try:
            confirmations = int(tx.get("confirmations", 0) or 0)
        except Exception:
            confirmations = 0
        if tx_status_code(tx) not in INACTIVE_TX_STATUSES and confirmations <= 0:
            add(txid)

    if cache is not None:
        try:
            operations = cache.list_operations(limit=200)
        except Exception:
            operations = []
        for op in operations:
            txid = str(op.get("txid", "") or "").strip()
            status = str(op.get("status", "") or "").strip().lower()
            if status in {"submitted", "queued", "executing"}:
                add(txid)
                continue
            if not txid or status != "success":
                continue
            try:
                entries = cache.get_transaction_entries(txid)
                max_confirmations = max(
                    (int(row.get("confirmations", 0) or 0) for row in entries),
                    default=1,
                )
            except Exception:
                max_confirmations = 0
            if max_confirmations <= 0:
                add(txid)
    return tracked


def merge_tx_status_update(row: dict, update: dict) -> bool:
    changed = False
    integer_fields = {"confirmations", "blockheight", "blockindex", "time", "blocktime", "timereceived"}
    for field in (
        "confirmations", "status", "blockhash", "blockheight", "blockindex",
        "time", "blocktime", "timereceived", "fee",
    ):
        if field not in update or update.get(field) in (None, ""):
            continue
        value = update.get(field)
        if field in integer_fields:
            try:
                value = int(value)
            except Exception:
                pass
        if row.get(field) != value:
            row[field] = value
            changed = True
    return changed


def with_wallet_operation_receives(
    txs: list,
    own_addresses: set[str],
    cache: WalletCache | None,
) -> list:
    rows = list(txs or [])
    if cache is None or not own_addresses:
        return rows
    existing = {
        (str(tx.get("txid", "") or ""), str(tx.get("category", "") or ""), str(tx.get("address", "") or ""))
        for tx in rows
    }
    by_txid = {str(tx.get("txid", "") or ""): tx for tx in rows if tx.get("txid")}
    try:
        operations = cache.list_operations(limit=500)
    except Exception:
        return rows
    for op in operations:
        txid = str(op.get("txid", "") or "").strip()
        from_address = str(op.get("from_address", "") or "").strip()
        to_address = str(op.get("to_address", "") or "").strip()
        if not txid or str(op.get("status", "") or "") != "success":
            continue
        base = by_txid.get(txid) or {}
        amount_zat = int(op.get("amount_zat") or 0)
        if amount_zat <= 0:
            continue
        common = {
            "txid": txid,
            "confirmations": int(base.get("confirmations", 0) or 0),
            "blockhash": base.get("blockhash", ""),
            "blockheight": base.get("blockheight"),
            "blockindex": base.get("blockindex"),
            "time": base.get("time") or base.get("blocktime") or base.get("timereceived") or op.get("created_at"),
            "blocktime": base.get("blocktime"),
            "timereceived": base.get("timereceived") or op.get("created_at"),
            "created_at": op.get("created_at"),
            "status": base.get("status", ""),
        }
        if from_address in own_addresses:
            key = (txid, "send", from_address)
            has_send = any(
                str(tx.get("txid", "") or "") == txid and tx.get("category") == "send"
                for tx in rows
            )
            if key not in existing and not has_send:
                fee_zat = int(op.get("fee_zat") or 0)
                rows.append({
                    **common,
                    "category": "send",
                    "address": from_address,
                    "amount": -zat_to_float(amount_zat),
                    "fee": -zat_to_float(fee_zat) if fee_zat else None,
                    "_synthetic": "own_operation_send",
                })
                existing.add(key)
        if to_address in own_addresses:
            key = (txid, "receive", to_address)
            if key not in existing:
                rows.append({
                    **common,
                    "category": "receive",
                    "address": to_address,
                    "amount": zat_to_float(amount_zat),
                    "_synthetic": "own_operation_receive",
                })
                existing.add(key)
    return rows
