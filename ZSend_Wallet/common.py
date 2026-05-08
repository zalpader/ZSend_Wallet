from __future__ import annotations

import sys
import os
import re
import time
import socket
import subprocess
import json
import io
import binascii
import random
import hashlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

_APP_ROOT = Path(__file__).resolve().parent.parent
_VENDOR_QR = _APP_ROOT / "_vendor_qr"
if _VENDOR_QR.exists():
    sys.path.insert(0, str(_VENDOR_QR))

import requests
from requests.auth import HTTPBasicAuth
try:
    import qrcode
except Exception:
    qrcode = None

from .version import DISPLAY_VERSION
from .wallet_cache import WalletCache, btcz_to_zat, zat_to_float
from .helpers import fmt_btcz, is_shielded_address

wallet_version = DISPLAY_VERSION
DEVELOPER_TIP_ADDRESS = "t1fjtqgoCboGToe6Mv68n5AvdDZaL6ZSend"


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = str(_APP_ROOT)
    return os.path.join(base_path, relative_path)


_SILENT_FLAGS: int = getattr(subprocess, "CREATE_NO_WINDOW", 0)

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QPushButton, QLabel,
    QLineEdit, QDoubleSpinBox, QSpinBox, QMessageBox, QHeaderView, QFrame,
    QDialog, QFormLayout, QDialogButtonBox, QComboBox,
    QProgressBar, QAbstractItemView, QTextEdit, QMenu,
    QStyledItemDelegate, QStyle, QStyleOptionComboBox, QScrollArea,
    QFileDialog, QTableView,
)
from PySide6.QtGui import QColor, QPainter, QFont, QAction, QIcon, QPixmap, QImage
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QRect, QEvent, QObject, QUrl, QAbstractTableModel, QModelIndex
from PySide6.QtNetwork import QLocalServer, QLocalSocket

from .debug_runtime import debug_exception, debug_log


WALLET_DIR = (
    Path(sys.executable).parent
    if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent.parent
)

DATA_DIR  = Path(os.environ.get("APPDATA", Path.home())) / "BitcoinZ"
CONF_PATH = DATA_DIR / "bitcoinz.conf"
EXPORT_DIR = DATA_DIR / "exports"

NODE_CANDIDATES = [
    WALLET_DIR / "bitcoinzd.exe",
    WALLET_DIR / "node" / "bitcoinzd.exe",
    WALLET_DIR / "bitcoinz" / "bitcoinzd.exe",
]

PARAMS_DIR = Path(os.environ.get("APPDATA", Path.home())) / "ZcashParams"

PARAMS_FILES = [
    {
        "name":   "sapling-output.params",
        "url":    "https://github.com/zalpader/ZSend_Wallet/releases/download/ZcashParams/sapling-output.params",
        "sha256": "2f0ebbcbb9bb0bcffe95a397e7eba89c29eb4dde6191c339db88570e3f3fb0e4",
        "size":   3592860,
    },
    {
        "name":   "sapling-spend.params",
        "url":    "https://github.com/zalpader/ZSend_Wallet/releases/download/ZcashParams/sapling-spend.params",
        "sha256": "8e48ffd23abb3a5fd9c5589204f32d9c31285a04b78096ba40a79b75677efc13",
        "size":   47958396,
    },
    {
        "name":   "sprout-groth16.params",
        "url":    "https://github.com/zalpader/ZSend_Wallet/releases/download/ZcashParams/sprout-groth16.params",
        "sha256": "b685d700c60328498fbde589c8c7c484c722b788b265b72af448a5bf0ee55b50",
        "size":   725523612,
    },
    {
        "name":   "sprout-proving.key",
        "url":    "https://github.com/zalpader/ZSend_Wallet/releases/download/ZcashParams/sprout-proving.key",
        "sha256": "8bc20a7f013b2b58970cddd2e7ea028975c88ae7ceb9259a5344a16bc2c0eef7",
        "size":   910173851,
    },
    {
        "name":   "sprout-verifying.key",
        "url":    "https://github.com/zalpader/ZSend_Wallet/releases/download/ZcashParams/sprout-verifying.key",
        "sha256": "4bd498dae0aacfd8e98dc306338d017d9c08dd0918ead18172bd0aec2fc5df82",
        "size":   1449,
    },
]
PARAMS_TOTAL_SIZE = sum(f["size"] for f in PARAMS_FILES)

_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_RNG = random.SystemRandom()


def _b58(n: int) -> str:
    return "".join(_RNG.choice(_B58) for _ in range(n))


_ADDNODES = [
    "152.53.180.255:1989",
    "51.222.50.26:1989",
]

_RPC_DEFAULT_PORT = 1979
_RPC_DEFAULT_HOST = "127.0.0.1"

_RUNNING_WORKERS: set = set()


def _mask_secret(value: str) -> str:
    if not value:
        return "<empty>"
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}***{value[-2:]} (len={len(value)})"


def _track(worker: "QThread") -> "QThread":
    _RUNNING_WORKERS.add(worker)
    worker.finished.connect(lambda: _RUNNING_WORKERS.discard(worker))
    return worker


_SINGLE_INSTANCE_KEY = "BitcoinZWallet_SingleInstance_v1"


def _is_z_addr(addr: str) -> bool:
    return is_shielded_address(addr)


def read_conf(path: Path = CONF_PATH) -> dict:
    result: dict = {}
    if not path.exists():
        return result
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    result[k.strip().lower()] = v.strip()
    except OSError:
        pass
    return result


def ensure_conf(path: Path = CONF_PATH) -> dict:
    debug_log("ensure_conf called", path=str(path), exists=path.exists())
    path.parent.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    existing = read_conf(path)

    if not path.exists():
        rpcuser = _b58(16)
        rpcpassword = _b58(32)
        lines = [
            f"rpcuser={rpcuser}",
            f"rpcpassword={rpcpassword}",
            "sendchangeback=1",
            f"exportdir={EXPORT_DIR.as_posix()}",
        ]
        for node in _ADDNODES:
            lines.append(f"addnode={node}")
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        except OSError:
            pass
    else:
        to_add: list[str] = []
        if not existing.get("rpcuser"):
            to_add.append(f"rpcuser={_b58(16)}")
        if not existing.get("rpcpassword"):
            to_add.append(f"rpcpassword={_b58(32)}")
        if "addnode" not in existing:
            for node in _ADDNODES:
                to_add.append(f"addnode={node}")
        if not existing.get("exportdir"):
            to_add.append(f"exportdir={EXPORT_DIR.as_posix()}")
        if to_add:
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write("\n" + "\n".join(to_add) + "\n")
            except OSError:
                pass

    cfg = read_conf(path)
    debug_log(
        "ensure_conf finished",
        path=str(path),
        rpcuser=_mask_secret(cfg.get("rpcuser", "")),
        rpcpassword=_mask_secret(cfg.get("rpcpassword", "")),
        exportdir=cfg.get("exportdir", ""),
        keys=sorted(cfg.keys()),
    )
    return cfg


def ensure_exportdir(path: Path = CONF_PATH) -> tuple[Path, bool]:
    cfg = ensure_conf(path)
    export_val = cfg.get("exportdir", "").strip()
    changed = False
    if export_val:
        export_path = Path(export_val)
    else:
        export_path = EXPORT_DIR
        changed = True
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"\nexportdir={export_path.as_posix()}\n")
        except OSError:
            pass
    export_path.mkdir(parents=True, exist_ok=True)
    debug_log("ensure_exportdir finished", export_path=str(export_path), changed=changed)
    return export_path, changed


def load_rpc_cfg() -> dict:
    c = ensure_conf()
    return {
        "host":       c.get("rpcbind", _RPC_DEFAULT_HOST),
        "port":       int(c.get("rpcport", _RPC_DEFAULT_PORT)),
        "user":       c.get("rpcuser",     ""),
        "password":   c.get("rpcpassword", ""),
        "exportdir":  c.get("exportdir", EXPORT_DIR.as_posix()),
        "conf_path":  str(CONF_PATH),
        "conf_found": CONF_PATH.exists(),
    }


def find_node():
    debug_log("Searching for node binary", candidates=[str(p) for p in NODE_CANDIDATES])
    for p in NODE_CANDIDATES:
        if p.exists():
            debug_log("Found node binary", path=str(p))
            return p
    debug_log("Node binary not found")
    return None


def node_running() -> bool:
    try:
        out = subprocess.check_output(
            ["tasklist", "/fo", "csv", "/nh"],
            stderr=subprocess.DEVNULL,
            creationflags=_SILENT_FLAGS,
            timeout=5,
        ).decode("utf-8", errors="replace").lower()
        running = "bitcoinzd" in out or "bitcoinz-qt" in out
        debug_log("node_running probe complete", running=running)
        return running
    except Exception as exc:
        debug_exception("node_running probe failed", exc)
        return False


def launch_node(binary: Path):
    try:
        export_dir, _ = ensure_exportdir()
        args = [str(binary), f"-conf={CONF_PATH}", f"-exportdir={export_dir}"]
        debug_log("Launching node process", args=args)
        proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_SILENT_FLAGS,
        )
        debug_log("Node launch started", pid=getattr(proc, "pid", None))
        return proc
    except Exception as exc:
        debug_exception("launch_node failed", exc)
        return None



def tx_ts(tx: dict) -> int:
    for f in ("blocktime", "time", "timereceived", "created_at"):
        v = tx.get(f)
        if v:
            try: return int(v)
            except Exception: pass
    return 0


def tx_fingerprint(txs: list) -> str:
    rows = []
    for tx in txs or []:
        rows.append("|".join([
            str(tx.get("txid", "")),
            str(tx.get("category", "")),
            str(tx.get("address", "")),
            fmt_btcz(float(tx.get("amount", 0) or 0)),
            fmt_btcz(float(tx.get("fee", 0) or 0)),
            str(tx.get("confirmations", "")),
            str(tx.get("blockhash", "")),
            str(tx.get("blockheight", "")),
            str(tx.get("blockindex", "")),
            str(tx_ts(tx)),
            str(tx.get("status", "")),
        ]))
    return "\n".join(rows)


def _sanitize_dump_basename(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value or "")
    return cleaned[:48] or "ZSendWalletExport"


def _representative_tx_address(entries: list[dict]) -> str:
    def addr_of(entry: dict) -> str:
        return str(entry.get("address") or "")

    receives = [entry for entry in entries if entry.get("category") == "receive" and addr_of(entry)]
    if receives:
        synthetic_receives = [entry for entry in receives if entry.get("_synthetic") == "own_operation_receive"]
        shielded_receives = [entry for entry in receives if _is_z_addr(addr_of(entry))]
        for bucket in (synthetic_receives, shielded_receives, receives):
            if bucket:
                return addr_of(bucket[0])

    sends = [entry for entry in entries if entry.get("category") == "send" and addr_of(entry)]
    if sends:
        return addr_of(sends[0])

    for entry in entries:
        if addr_of(entry):
            return addr_of(entry)
    return ""


def group_tx_rows(txs: list) -> list:
    grouped: dict[str, dict] = {}
    order: list[str] = []
    for tx in txs or []:
        txid = tx.get("txid", "")
        key = txid or f"__row_{len(order)}"
        if key not in grouped:
            grouped[key] = dict(tx)
            grouped[key]["_entries"] = [tx]
            order.append(key)
            continue
        g = grouped[key]
        g["_entries"].append(tx)
        try:
            g["amount"] = float(g.get("amount", 0) or 0) + float(tx.get("amount", 0) or 0)
        except Exception:
            pass
        if not g.get("address") and tx.get("address"):
            g["address"] = tx.get("address")
        if not g.get("category") and tx.get("category"):
            g["category"] = tx.get("category")
        if tx_ts(tx) > tx_ts(g):
            for field in ("time", "blocktime", "timereceived"):
                if tx.get(field):
                    g[field] = tx.get(field)
        try:
            g["confirmations"] = min(
                int(g.get("confirmations", 0) or 0),
                int(tx.get("confirmations", 0) or 0),
            )
        except Exception:
            pass
        if tx.get("fee") is not None:
            g["fee"] = tx.get("fee")
        for field in ("blockhash", "blockheight", "blockindex", "status"):
            if tx.get(field) not in (None, ""):
                g[field] = tx.get(field)
    rows = [grouped[k] for k in order]
    for row in rows:
        entries = row.get("_entries") or []
        representative = _representative_tx_address(entries)
        if representative:
            row["address"] = representative
        has_send = any(entry.get("category") == "send" for entry in entries)
        has_receive = any(entry.get("category") == "receive" for entry in entries)
        if has_send and has_receive and row.get("fee") is not None:
            try:
                row["amount"] = float(row.get("amount", 0) or 0) + float(row.get("fee", 0) or 0)
            except Exception:
                pass
    return rows


def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


_NODE_MSGS = {
    "Loading block index":        "Loading block index",
    "Activating best chain":      "Activating best chain",
    "Rewinding blocks if needed": "Rewinding blocks",
    "Loading wallet":             "Loading wallet",
    "Rescanning":                 "Rescanning blockchain",
    "Verifying blocks":           "Verifying blocks",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


_REINDEX_PHRASES = ("reindexing", "while reindexing", "disabled while", "reindex")


def _is_reindex_err(e) -> bool:
    return any(p in str(e).lower() for p in _REINDEX_PHRASES)
