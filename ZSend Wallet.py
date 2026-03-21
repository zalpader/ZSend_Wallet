import sys
import os
import re
import time
import socket
import subprocess
import json
import binascii
import random
import hashlib
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

wallet_version: float = 0.85


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


_SILENT_FLAGS: int = getattr(subprocess, "CREATE_NO_WINDOW", 0)

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableWidget, QTableWidgetItem, QPushButton, QLabel,
    QLineEdit, QDoubleSpinBox, QMessageBox, QHeaderView, QFrame,
    QDialog, QFormLayout, QDialogButtonBox, QComboBox,
    QProgressBar, QAbstractItemView, QTextEdit, QMenu,
    QStyledItemDelegate, QStyle, QStyleOptionComboBox, QScrollArea,
)
from PySide6.QtGui import QColor, QPainter, QFont, QAction, QIcon
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QRect, QEvent, QObject
from PySide6.QtNetwork import QLocalServer, QLocalSocket


WALLET_DIR = (
    Path(sys.executable).parent
    if getattr(sys, "frozen", False)
    else Path(__file__).parent
)

DATA_DIR  = Path(os.environ.get("APPDATA", Path.home())) / "BitcoinZ"
CONF_PATH = DATA_DIR / "bitcoinz.conf"

NODE_CANDIDATES = [
    WALLET_DIR / "bitcoinzd.exe",
    WALLET_DIR / "node" / "bitcoinzd.exe",
    WALLET_DIR / "bitcoinz" / "bitcoinzd.exe",
]

PARAMS_DIR = Path(os.environ.get("APPDATA", Path.home())) / "ZcashParams"

PARAMS_FILES = [
    {
        "name":   "sapling-output.params",
        "url":    "https://d.btcz.rocks/sapling-output.params",
        "sha256": "2f0ebbcbb9bb0bcffe95a397e7eba89c29eb4dde6191c339db88570e3f3fb0e4",
        "size":   3592860,
    },
    {
        "name":   "sapling-spend.params",
        "url":    "https://d.btcz.rocks/sapling-spend.params",
        "sha256": "8e48ffd23abb3a5fd9c5589204f32d9c31285a04b78096ba40a79b75677efc13",
        "size":   47958360,
    },
    {
        "name":   "sprout-groth16.params",
        "url":    "https://d.btcz.rocks/sprout-groth16.params",
        "sha256": "b685d700c60328498fbde589c8c7c484c722b788b265b72af448a5bf0ee55b50",
        "size":   725523612,
    },
    {
        "name":   "sprout-proving.key",
        "url":    "https://d.btcz.rocks/sprout-proving.key",
        "sha256": "8bc20a7f013b2b58970cddd2e7ea028975c88ae7ceb9259a5344a16bc2c0eef7",
        "size":   910173851,
    },
    {
        "name":   "sprout-verifying.key",
        "url":    "https://d.btcz.rocks/sprout-verifying.key",
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


def _track(worker: "QThread") -> "QThread":
    _RUNNING_WORKERS.add(worker)
    worker.finished.connect(lambda: _RUNNING_WORKERS.discard(worker))
    return worker


_SINGLE_INSTANCE_KEY = "BitcoinZWallet_SingleInstance_v1"


def _is_z_addr(addr: str) -> bool:
    return addr.startswith("zs") or addr.startswith("zc") or len(addr) > 40


def _fmt_addr(addr: str) -> str:
    if _is_z_addr(addr):
        return "🛡 " + addr[:18] + "…" + addr[-18:]
    return addr


def _sort_addr_items(items: list, mode: str) -> list:
    if mode == 'name':
        return sorted(items, key=lambda x: x[0].lower())
    non_zero = sorted([(a, b) for a, b in items if b > 0],  key=lambda x: x[1], reverse=True)
    zero     = sorted([(a, b) for a, b in items if b <= 0], key=lambda x: x[0].lower())
    return non_zero + zero


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
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_conf(path)

    if not path.exists():
        rpcuser = _b58(16)
        rpcpassword = _b58(32)
        lines = [
            f"rpcuser={rpcuser}",
            f"rpcpassword={rpcpassword}",
            "sendchangeback=1",
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
        if to_add:
            try:
                with open(path, "a", encoding="utf-8") as f:
                    f.write("\n" + "\n".join(to_add) + "\n")
            except OSError:
                pass

    return read_conf(path)


def load_rpc_cfg() -> dict:
    c = ensure_conf()
    return {
        "host":       c.get("rpcbind", _RPC_DEFAULT_HOST),
        "port":       int(c.get("rpcport", _RPC_DEFAULT_PORT)),
        "user":       c.get("rpcuser",     ""),
        "password":   c.get("rpcpassword", ""),
        "conf_path":  str(CONF_PATH),
        "conf_found": CONF_PATH.exists(),
    }


def find_node():
    for p in NODE_CANDIDATES:
        if p.exists():
            return p
    return None


def node_running() -> bool:
    try:
        out = subprocess.check_output(
            ["tasklist", "/fo", "csv", "/nh"],
            stderr=subprocess.DEVNULL,
            creationflags=_SILENT_FLAGS,
            timeout=5,
        ).decode("utf-8", errors="replace").lower()
        return "bitcoinzd" in out or "bitcoinz-qt" in out
    except Exception:
        return False


def launch_node(binary: Path):
    try:
        args = [str(binary), f"-conf={CONF_PATH}"]
        return subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_SILENT_FLAGS,
        )
    except Exception:
        return None


class RPCError(Exception):
    def __init__(self, msg: str, code: int = 0):
        super().__init__(msg)
        self.code = code


class BitcoinZRPC:

    def __init__(self, host="127.0.0.1", port=1979, user="", password=""):
        self.host     = host
        self.port     = port
        self.user     = user
        self.password = password
        self.url      = f"http://{host}:{port}/"
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(user, password)
        self._session.headers.update({"Content-Type": "text/plain"})
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=8, pool_maxsize=8, max_retries=0
        )
        self._session.mount("http://", adapter)
        self._lock = __import__("threading").Lock()

    def call(self, method: str, params: list, timeout: int = 30):
        body = json.dumps({
            "jsonrpc": "1.0",
            "id":      "curltest",
            "method":  method,
            "params":  params,
        })
        try:
            r = self._session.post(self.url, data=body, timeout=timeout)
        except requests.exceptions.ConnectionError:
            raise RPCError(f"Connection refused → {self.url}")
        except requests.exceptions.Timeout:
            raise RPCError(f"[{method}] timed out after {timeout}s")
        except Exception as e:
            raise RPCError(str(e))

        if r.status_code == 401:
            raise RPCError("HTTP 401 – rpcuser/rpcpassword mismatch", 401)
        if r.status_code == 403:
            raise RPCError("HTTP 403 – rpcallowip not set in bitcoinz.conf", 403)

        try:
            data = r.json()
        except ValueError:
            raise RPCError(f"Non-JSON response: {r.text[:200]}")

        err = data.get("error")
        if err:
            code = err.get("code", 0) if isinstance(err, dict) else 0
            msg  = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            raise RPCError(msg, code)

        return data.get("result")

    def getInfo(self):             return self.call("getinfo", [])
    def getBlockchainInfo(self):   return self.call("getblockchaininfo", [])
    def getWalletInfo(self):       return self.call("getwalletinfo", [])
    def getNewAddress(self) -> str:   return self.call("getnewaddress", [])
    def z_getNewAddress(self) -> str: return self.call("z_getnewaddress", [])

    def ListAddresses(self) -> list:
        try:
            result = self.call("listaddresses", [])
            if isinstance(result, list):
                return result
        except RPCError:
            pass
        try:
            rows = self.call("listreceivedbyaddress", [0, True])
            return [r["address"] for r in rows]
        except RPCError:
            pass
        try:
            return [self.call("getnewaddress", [])]
        except RPCError:
            return []

    def z_listAddresses(self) -> list:  return self.call("z_listaddresses", [])
    def z_getTotalBalance(self) -> dict: return self.call("z_gettotalbalance", [0])
    def z_getBalance(self, address: str) -> float:
        return float(self.call("z_getbalance", [address, 0]))

    def listTransactions(self, count: int, tx_from: int) -> list:
        return self.call("listtransactions", ["*", count, tx_from])

    def z_sendMany(self, uaddress: str, toaddress: str, amount: float, txfee: float) -> str:
        return self.call("z_sendmany", [
            uaddress,
            [{"address": toaddress, "amount": float(amount)}],
            1, float(txfee),
        ])

    def SendMemo(self, uaddress: str, toaddress: str, amount: float, txfee: float, memo: str) -> str:
        hex_memo = binascii.hexlify(memo.encode()).decode()
        return self.call("z_sendmany", [
            uaddress,
            [{"address": toaddress, "amount": float(amount), "memo": hex_memo}],
            1, float(txfee),
        ])

    def z_getOperationStatus(self, opid: str) -> list:
        return self.call("z_getoperationstatus", [[opid]])

    def z_getOperationResult(self, opid: str) -> list:
        return self.call("z_getoperationresult", [[opid]])

    def DumpPrivKey(self, address: str) -> str: return self.call("dumpprivkey", [address])
    def z_ExportKey(self, address: str) -> str:  return self.call("z_exportkey", [address])

    def ImportPrivKey(self, key: str) -> None:
        self.call("importprivkey", [key], timeout=7200)

    def z_ImportKey(self, key: str) -> dict:
        return self.call("z_importkey", [key, "whenkeyisnew", 0], timeout=7200)

    def getTransaction(self, txid: str) -> dict:    return self.call("gettransaction", [txid])
    def getRawTransaction(self, txid: str) -> dict: return self.call("getrawtransaction", [txid, 1])
    def stopNode(self):                             return self.call("stop", [])


def tx_ts(tx: dict) -> int:
    for f in ("blocktime", "time", "timereceived"):
        v = tx.get(f)
        if v:
            try: return int(v)
            except Exception: pass
    return 0


def fmt_ts(ts) -> str:
    if not ts: return "-"
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d  %H:%M")
    except Exception:
        return str(ts)


def fmt_btcz(v: float) -> str:
    if v == 0:
        return "0"
    return f"{v:.8f}".rstrip("0").rstrip(".")


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


class ParamsWorker(QThread):
    status   = Signal(str)
    progress = Signal(int, int)
    done     = Signal()
    failed   = Signal(str)

    def run(self):
        PARAMS_DIR.mkdir(parents=True, exist_ok=True)
        self.status.emit("Checking ZcashParams")

        corrupt = []
        for pf in PARAMS_FILES:
            path = PARAMS_DIR / pf["name"]
            if path.exists():
                self.status.emit(f"Verifying {pf['name']}")
                if _sha256_file(path) != pf["sha256"]:
                    corrupt.append(pf["name"])
                    path.unlink()

        if corrupt:
            self.status.emit(f"Corrupt files removed, re-downloading: {', '.join(corrupt)}")

        to_download = [pf for pf in PARAMS_FILES if not (PARAMS_DIR / pf["name"]).exists()]

        if not to_download:
            self.status.emit("ZcashParams OK.")
            self.done.emit()
            return

        total      = sum(pf["size"] for pf in to_download)
        done_bytes = 0

        for pf in to_download:
            path    = PARAMS_DIR / pf["name"]
            attempt = 0

            while attempt < 2:
                attempt += 1
                existing = path.stat().st_size if path.exists() else 0
                self.status.emit(
                    f"Downloading {pf['name']} ({'resuming' if existing else 'starting'})"
                )
                try:
                    headers = {"Range": f"bytes={existing}-"} if existing else {}
                    with requests.get(pf["url"], headers=headers, stream=True, timeout=30) as r:
                        if r.status_code not in (200, 206):
                            raise IOError(f"HTTP {r.status_code}")
                        mode = "ab" if existing and r.status_code == 206 else "wb"
                        if mode == "wb":
                            existing = 0
                        with open(path, mode) as f:
                            for chunk in r.iter_content(1 << 16):
                                if not chunk: continue
                                f.write(chunk)
                                existing   += len(chunk)
                                done_bytes += len(chunk)
                                self.progress.emit(done_bytes, PARAMS_TOTAL_SIZE)
                except Exception as e:
                    if attempt >= 2:
                        self.failed.emit(f"Failed to download {pf['name']}:\n{e}")
                        return

                self.status.emit(f"Verifying {pf['name']}")
                if _sha256_file(path) == pf["sha256"]:
                    break
                else:
                    if attempt >= 2:
                        self.failed.emit(
                            f"File {pf['name']} is corrupted after download.\n"
                            "Please check your internet connection and try again."
                        )
                        return
                    if path.exists(): path.unlink()
                    existing = 0
                    self.status.emit(f"Hash mismatch for {pf['name']}, retrying")

        self.status.emit("ZcashParams OK.")
        self.done.emit()


class NodeStartWorker(QThread):
    status = Signal(str)
    ready  = Signal()
    failed = Signal(str)

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__(); self.rpc = rpc

    def run(self):
        self.status.emit("Checking node status")

        if not node_running():
            binary = find_node()
            if binary:
                self.status.emit(f"Starting {binary.name}")
                launch_node(binary)
                time.sleep(5)
            else:
                self.status.emit("bitcoinzd.exe not found - waiting for manual start")

        for attempt in range(120):
            try:
                self.rpc.getInfo()
                self.status.emit("Node is ready!")
                self.ready.emit()
                return
            except RPCError as e:
                msg  = str(e)
                code = e.code

                if code == 401:
                    self.failed.emit(
                        "HTTP 401: rpcuser/rpcpassword mismatch with bitcoinz.conf.\n"
                        "Delete bitcoinz.conf and restart the wallet to regenerate it."
                    )
                    return
                if code == 403:
                    self.failed.emit(
                        "HTTP 403: node is rejecting connections.\n"
                        "Add these lines to bitcoinz.conf:\n"
                        "  server=1\n"
                        "  rpcallowip=127.0.0.1\n"
                        "then restart the node."
                    )
                    return

                if code == -28 or any(k in msg for k in _NODE_MSGS):
                    display = next(
                        (v for k, v in _NODE_MSGS.items() if k in msg),
                        msg.splitlines()[0]
                    )
                    self.status.emit(display)
                elif "Connection refused" in msg or "refused" in msg.lower():
                    self.status.emit(f"Waiting for node ({attempt * 3}s)")
                else:
                    self.status.emit(f"Waiting ({msg.splitlines()[0][:60]})")

            time.sleep(3)

        self.failed.emit(
            "Node did not respond within 360 seconds.\n"
            "Check that bitcoinzd.exe is present and bitcoinz.conf is correct.\n"
            "Open Diagnostics for details."
        )


_REINDEX_PHRASES = ("reindexing", "while reindexing", "disabled while", "reindex")


def _is_reindex_err(e: RPCError) -> bool:
    return any(p in str(e).lower() for p in _REINDEX_PHRASES)


class RefreshWorker(QThread):
    done       = Signal(object)
    error      = Signal(str)
    reindexing = Signal(object)
    step       = Signal(str)

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__(); self.rpc = rpc

    def _reindex_emit(self, info, chain, t_addrs=(), z_addrs=(),
                      t_bal=None, z_bal=None, total_bal=None):
        self.reindexing.emit({
            "info": info, "chain": chain,
            "t_addrs": list(t_addrs), "z_addrs": list(z_addrs),
            "t_balances": t_bal or {}, "z_balances": z_bal or {},
            "total_bal": total_bal or {}, "txs": [],
            "reindexing": True,
        })

    def run(self):
        try:
            self.step.emit("getinfo")
            info = self.rpc.getInfo()

            self.step.emit("getblockchaininfo")
            try:
                chain = self.rpc.getBlockchainInfo()
            except Exception:
                chain = {}

            if chain.get("reindex", False) or chain.get("initialblockdownload", False):
                self._reindex_emit(info, chain)
                return

            self.step.emit("t-addresses")
            try:
                t_addrs = self.rpc.ListAddresses()
            except RPCError as e:
                if _is_reindex_err(e):
                    self._reindex_emit(info, chain)
                    return
                raise

            self.step.emit("z-addresses")
            try:
                z_addrs = self.rpc.z_listAddresses()
            except RPCError as e:
                if _is_reindex_err(e):
                    self._reindex_emit(info, chain, t_addrs)
                    return
                raise

            self.step.emit("total balance")
            try:
                total_bal = self.rpc.z_getTotalBalance()
            except RPCError as e:
                if _is_reindex_err(e):
                    self._reindex_emit(info, chain, t_addrs, z_addrs)
                    return
                raise

            self.step.emit("transactions")
            try:
                txs = self.rpc.listTransactions(200, 0)
            except RPCError as e:
                if _is_reindex_err(e):
                    self._reindex_emit(info, chain, t_addrs, z_addrs, total_bal=total_bal)
                    return
                raise

            self.step.emit("address balances")

            def _fetch_bal(addr):
                try:
                    return addr, self.rpc.z_getBalance(addr)
                except RPCError as e:
                    return addr, e
                except Exception:
                    return addr, 0.0

            all_addrs = list(t_addrs) + list(z_addrs)
            t_bal: dict = {}
            z_bal: dict = {}
            bal_error: str = ""
            if all_addrs:
                with ThreadPoolExecutor(max_workers=min(6, len(all_addrs))) as ex:
                    futures = {ex.submit(_fetch_bal, a): a for a in all_addrs}
                    for fut in as_completed(futures):
                        addr, bal = fut.result()
                        if isinstance(bal, RPCError):
                            if not bal_error and bal.code in (401, 403):
                                bal_error = str(bal)
                            bal = 0.0
                        if addr in t_addrs:
                            t_bal[addr] = bal
                        else:
                            z_bal[addr] = bal
            if bal_error:
                self.error.emit(f"Balance fetch error: {bal_error}")
                return

            self.done.emit({
                "info": info, "chain": chain,
                "t_addrs": t_addrs, "z_addrs": z_addrs,
                "t_balances": t_bal, "z_balances": z_bal,
                "total_bal": total_bal, "txs": txs,
                "reindexing": False,
            })

        except RPCError as e:
            self.error.emit(str(e))
        except Exception:
            self.error.emit(traceback.format_exc())


class PollWorker(QThread):
    status_update = Signal(str)
    success       = Signal(str)
    failed        = Signal(str)

    def __init__(self, rpc: BitcoinZRPC, opid: str):
        super().__init__()
        self.rpc   = rpc
        self.opid  = opid
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        while not self._stop:
            try:
                res = self.rpc.z_getOperationStatus(self.opid)
                if res and isinstance(res, list) and res:
                    s = res[0].get("status", "")
                    if s == "success":
                        res2 = self.rpc.z_getOperationResult(self.opid)
                        txid = ""
                        if res2 and isinstance(res2, list) and res2:
                            txid = res2[0].get("result", {}).get("txid", "")
                        self.success.emit(txid or "?")
                        return
                    elif s == "failed":
                        err = res[0].get("error", {})
                        msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                        self.failed.emit(msg)
                        return
                    else:
                        self.status_update.emit(f"Status: {s}")
                else:
                    self.status_update.emit("Waiting for confirmation")
            except RPCError as e:
                self.status_update.emit(f"Polling error: {e}")
            except Exception:
                pass
            for _ in range(8):
                if self._stop: return
                time.sleep(0.5)


class SendWorker(QThread):
    done  = Signal(str)
    error = Signal(str)

    def __init__(self, rpc: BitcoinZRPC, frm: str, to: str,
                 amount: float, fee: float, memo: str = ""):
        super().__init__()
        self.rpc = rpc; self.frm = frm; self.to = to
        self.amount = amount; self.fee = fee; self.memo = memo

    def run(self):
        try:
            if self.memo:
                opid = self.rpc.SendMemo(self.frm, self.to, self.amount, self.fee, self.memo)
            else:
                opid = self.rpc.z_sendMany(self.frm, self.to, self.amount, self.fee)
            self.done.emit(opid)
        except RPCError as e:
            self.error.emit(str(e))


class ImportKeyWorker(QThread):
    progress       = Signal(str)
    done           = Signal(str)
    error          = Signal(str)
    z_key_accepted = Signal(str)

    def __init__(self, rpc: BitcoinZRPC, key: str, is_z: bool):
        super().__init__()
        self.rpc = rpc; self.key = key; self.is_z = is_z

    def run(self):
        try:
            if self.is_z:
                self.progress.emit("Importing shielded key")
                result = self.rpc.z_ImportKey(self.key)
                addr   = (result or {}).get("address", "")
                self.z_key_accepted.emit(addr)
            else:
                self.progress.emit(
                    "Importing private key may take a few minutes! Please wait."
                )
                self.rpc.ImportPrivKey(self.key)
                self.done.emit("Transparent private key imported successfully")
        except RPCError as e:
            self.error.emit(str(e))


class LogTailWorker(QThread):
    progress = Signal(float, int)
    done     = Signal()

    _DONE_IDLE_SECS = 120

    def __init__(self):
        super().__init__()
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self):
        log_path = DATA_DIR / "debug.log"
        try:
            last_pos = log_path.stat().st_size
        except OSError:
            last_pos = 0

        last_progress_time: float = 0.0
        seen_progress = False

        while not self._stop:
            time.sleep(1)
            try:
                with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                    f.seek(last_pos)
                    while True:
                        line = f.readline()
                        if not line:
                            last_pos = f.tell()
                            break
                        m_prog  = re.search(r"Progress=(\d+\.\d+)", line)
                        m_block = re.search(r"block[^\d]*(\d+)", line, re.IGNORECASE)
                        if m_prog:
                            frac  = float(m_prog.group(1))
                            block = int(m_block.group(1)) if m_block else 0
                            self.progress.emit(frac, block)
                            last_progress_time = time.time()
                            seen_progress = True
            except OSError:
                pass

            if (seen_progress
                    and last_progress_time > 0
                    and time.time() - last_progress_time > self._DONE_IDLE_SECS
                    and not self._stop):
                self.done.emit()
                return


class ShutdownWorker(QThread):
    status = Signal(str)
    done   = Signal()

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__(); self.rpc = rpc

    def run(self):
        self.status.emit("Sending stop command to node")
        try:
            self.rpc.stopNode()
        except RPCError:
            pass

        for i in range(30):
            time.sleep(1)
            if not node_running():
                self.status.emit("Node stopped.")
                self.done.emit()
                return
            self.status.emit(f"Waiting for node to stop ({i + 1}s)")

        self.status.emit("Node did not stop in time - exiting anyway.")
        self.done.emit()


QSS = """
QMainWindow,QDialog{background:#1c2128;color:#e6edf3}
QDialog{border:1px solid #444c56;border-radius:8px}
QWidget{background:#1c2128;color:#e6edf3;font-family:'Segoe UI',sans-serif;font-size:10pt}
QTabWidget::pane{border:1px solid #30363d;border-radius:6px;background:#1c2128}
QTabBar::tab{background:#1c2128;color:#8b949e;padding:10px 18px;border:none;
  font-weight:600;font-size:13px;min-width:100px}
QTabBar::tab:selected{color:#f7a32c;border-bottom:3px solid #f7a32c;background:#1c2128}
QTabBar::tab:hover{color:#e6edf3}
QTableWidget{background:#1c2128;alternate-background-color:#1c2128;color:#e6edf3;
  font-size:13px;gridline-color:#30363d;border:1px solid #30363d;border-radius:6px;
  selection-background-color:#2d333b;outline:0}
QTableWidget::item{padding:4px 6px;font-size:13px;border:none;outline:0}
QTableWidget::item:selected{background:#2d333b;color:#f7a32c;font-size:13px;
  padding:4px 6px;border:none;outline:0}
QTableWidget::item:focus{padding:4px 6px;border:none;outline:0}
QTableWidget::item:selected:focus{padding:4px 6px;border:none;outline:0}
QPushButton{background:#21262d;color:#e6edf3;border:1px solid #30363d;
  border-radius:6px;padding:8px 18px;font-weight:600}
QPushButton:hover{background:#2d333b;border-color:#58a6ff;color:#58a6ff}
QPushButton:pressed{background:#1f6feb;color:#fff}
QPushButton#primary{background:#f7a32c;color:#1c2128;border:none;font-weight:700}
QPushButton#primary:hover{background:#fbbe5e}
QPushButton#primary:pressed{background:#d68e20}
QPushButton#primary:disabled{background:#30363d;color:#666;border:none;font-weight:700}
QPushButton#shield{background:#238636;color:#fff;border:none;font-weight:700}
QPushButton#shield:hover{background:#2ea043}
QPushButton#danger{background:#21262d;color:#f85149;border:1px solid #f85149;font-weight:700}
QPushButton#danger:hover{background:#2d1e1e}
QLineEdit,QDoubleSpinBox,QComboBox,QSpinBox{background:#21262d;color:#e6edf3;
  border:1px solid #30363d;border-radius:6px;padding:8px 10px;font-size:11pt}
QDoubleSpinBox,QSpinBox{padding-top:0;padding-bottom:0}
QDoubleSpinBox::up-button,QSpinBox::up-button,
QDoubleSpinBox::down-button,QSpinBox::down-button{width:0;height:0;border:none}
QDoubleSpinBox::up-arrow,QSpinBox::up-arrow,
QDoubleSpinBox::down-arrow,QSpinBox::down-arrow{width:0;height:0;image:none}
QLineEdit:focus,QDoubleSpinBox:focus,QComboBox:focus,QSpinBox:focus{border-color:#58a6ff}
QComboBox::drop-down{border:none}
QComboBox QAbstractItemView{
  background:#21262d;color:#e6edf3;
  selection-background-color:#2d333b;
  font-family:'Segoe UI',sans-serif;font-size:11pt;
  outline:0}
QLabel{color:#e6edf3}
QFrame#card{background:#1c2128;border:1px solid #30363d;border-radius:10px}
QFrame#div{background:#30363d;max-height:1px}
QStatusBar{background:#1c2128;color:#8b949e;font-size:12px}
QScrollBar:vertical{background:#1c2128;width:8px;margin:0}
QScrollBar::handle:vertical{background:#30363d;border-radius:4px;min-height:20px}
QScrollBar::add-line,QScrollBar::sub-line{height:0}
QProgressBar{background:#21262d;border:none;border-radius:4px;height:6px}
QProgressBar::chunk{background:#f7a32c;border-radius:4px}
QProgressBar#sbar{background:#21262d;border:1px solid #30363d;border-radius:4px;
  height:16px;text-align:center;color:#e6edf3;font-size:11px}
QProgressBar#sbar::chunk{background:#f7a32c;border-radius:3px}
QProgressBar#rbar{background:#21262d;border:1px solid #30363d;border-radius:4px;
  height:16px;text-align:center;color:#e6edf3;font-size:11px}
QProgressBar#rbar::chunk{background:#1f6feb;border-radius:3px}
QTextEdit{background:#1c2128;color:#c9d1d9;border:1px solid #30363d;border-radius:6px;
  font-family:Consolas,'Courier New',monospace;font-size:12px}
QMenuBar{background:#1c2128;color:#e6edf3;font-size:13px;padding:0 2px}
QMenuBar::item{padding:4px 8px;background:transparent}
QMenuBar::item:selected{background:#21262d;border-radius:4px}
QMenuBar::item:pressed{background:#21262d}
QMenu{background:#21262d;color:#e6edf3;border:1px solid #30363d;font-size:13px}
QMenu::item{padding:6px 20px 6px 12px}
QMenu::item:selected{background:#2d333b}
QMenu::item:disabled{color:#484f58}
"""


class _DraggableDialog(QDialog):

    def __init__(self, parent=None):
        super().__init__(parent)
        self._drag_pos = None
        self.setWindowFlags(Qt.WindowType.Dialog | Qt.WindowType.FramelessWindowHint)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag_pos = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._drag_pos is not None and event.buttons() & Qt.MouseButton.LeftButton:
            self.move(event.globalPosition().toPoint() - self._drag_pos)
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        self._drag_pos = None
        super().mouseReleaseEvent(event)


class ConfigDialog(_DraggableDialog):
    def __init__(self, parent, host, port, user, pw, conf_path=""):
        super().__init__(parent)
        self.setWindowTitle("RPC Connection Settings"); self.setMinimumWidth(500)
        f = QFormLayout(self); f.setSpacing(12); f.setContentsMargins(18, 18, 18, 18)
        if conf_path:
            h = QLabel(f"Config: {conf_path}"); h.setStyleSheet("color:#8b949e;font-size:11px;")
            f.addRow(h)
        self.e_host = QLineEdit(host); self.e_port = QLineEdit(str(port))
        self.e_user = QLineEdit(user); self.e_pass = QLineEdit(pw)
        self.e_pass.setEchoMode(QLineEdit.EchoMode.Password)
        f.addRow("RPC Host:",     self.e_host)
        f.addRow("RPC Port:",     self.e_port)
        f.addRow("RPC User:",     self.e_user)
        f.addRow("RPC Password:", self.e_pass)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject); f.addRow(btns)

    def values(self):
        return (self.e_host.text().strip(), int(self.e_port.text().strip() or "1979"),
                self.e_user.text().strip(), self.e_pass.text().strip())


class DiagDialog(_DraggableDialog):
    def __init__(self, parent, rpc: BitcoinZRPC):
        super().__init__(parent)
        self.setWindowTitle("Full Diagnostics"); self.setMinimumSize(720, 560)
        self._rpc = rpc
        self._diag_worker = None
        v = QVBoxLayout(self); v.setContentsMargins(18, 18, 18, 18); v.setSpacing(10)
        t = QLabel("Node & RPC Diagnostics"); t.setStyleSheet("font-size:15px;font-weight:700;")
        v.addWidget(t)
        self.txt = QTextEdit(); self.txt.setReadOnly(True)
        self.txt.setPlainText("Running diagnostics")
        v.addWidget(self.txt, 1)
        h = QHBoxLayout()
        self.btn_rerun = QPushButton("Refresh"); self.btn_rerun.clicked.connect(self._start)
        h.addWidget(self.btn_rerun)
        self.btn_copy = QPushButton("Copy to Clipboard")
        self.btn_copy.clicked.connect(self._copy_report)
        h.addWidget(self.btn_copy)
        h.addStretch()
        bc = QPushButton("Close"); bc.clicked.connect(self.accept); h.addWidget(bc)
        v.addLayout(h)
        self._start()

    def _start(self):
        if self._diag_worker is not None:
            try: self._diag_worker.done.disconnect()
            except Exception: pass
            self._diag_worker = None
        self.btn_rerun.setEnabled(False)
        self.btn_copy.setEnabled(False)
        self.txt.setPlainText("Running diagnostics")
        self._diag_worker = _DiagWorker(self._rpc)
        self._diag_worker.done.connect(self._on_done)
        _track(self._diag_worker).start()

    def _on_done(self, text: str):
        self.txt.setPlainText(text)
        self.btn_rerun.setEnabled(True)
        self.btn_copy.setEnabled(True)

    def _copy_report(self):
        QApplication.clipboard().setText(self.txt.toPlainText())
        self.btn_copy.setText("Copied!")
        QTimer.singleShot(2000, lambda: _safe_set_text(self.btn_copy, "Copy to Clipboard"))


class _DiagWorker(QThread):
    done = Signal(str)

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__(); self.rpc = rpc

    def run(self):
        lines = []
        rpc = self.rpc
        cfg = load_rpc_cfg()
        lines.append("--- Config ---------------------------------------------------")
        if cfg["conf_found"]:
            import re as _re
            safe_path = _re.sub(r'(?i)([\\/]Users[\\/])[^\\/]+', r'\1***', cfg['conf_path'])
            lines.append(f"  OK  {safe_path}")
            c = read_conf()
            v_user = c.get("rpcuser", "")
            v_pass = c.get("rpcpassword", "")
            v_node = c.get("addnode", "")
            lines.append(f"  {'OK' if v_user else '!!'  }  {'rpcuser':<14}= {v_user if v_user else '<MISSING>'}")
            lines.append(f"  {'OK' if v_pass else '!!'  }  {'rpcpassword':<14}= {'***' if v_pass else '<MISSING>'}")
            lines.append(f"  {'OK' if v_node else '!!'  }  {'addnode':<14}= {v_node if v_node else '<MISSING>'}")
            rpcport_val = c.get("rpcport")
            lines.append(f"  OK  {'rpcport':<14}= "
                         f"{rpcport_val if rpcport_val else str(_RPC_DEFAULT_PORT) + '  (default)'}")
            rpcip_val = c.get("rpcallowip")
            lines.append(f"  OK  {'rpcallowip':<14}= "
                         f"{rpcip_val if rpcip_val else _RPC_DEFAULT_HOST + '  (default)'}")
        else:
            lines.append(f"  !!  NOT found: {cfg['conf_path']}")
        lines.append("\n--- Node binary ----------------------------------------------")
        nb = find_node()
        lines.append(f"  {'OK' if nb else '!!'} {nb.name if nb else 'bitcoinzd.exe NOT found'}")
        lines.append("\n--- Process --------------------------------------------------")
        lines.append("  OK  bitcoinzd running" if node_running() else "  !!  not running")
        lines.append("\n--- TCP port -------------------------------------------------")
        open_ = is_port_open(rpc.host, rpc.port)
        lines.append(f"  {'OK' if open_ else '!!'} port {rpc.port} "
                     f"{'open' if open_ else 'CLOSED (node still loading?)'}")
        lines.append("\n--- RPC methods ----------------------------------------------")
        for name, fn, show_data in [
            ("getinfo",           rpc.getInfo,                         True),
            ("getblockchaininfo", rpc.getBlockchainInfo,               True),
            ("getwalletinfo",     rpc.getWalletInfo,                   False),
            ("listaddresses",     rpc.ListAddresses,                   False),
            ("z_listaddresses",   rpc.z_listAddresses,                 False),
            ("z_gettotalbalance", rpc.z_getTotalBalance,               False),
            ("listtransactions",  lambda: rpc.listTransactions(3, 0),  False),
        ]:
            try:
                result = fn()
                if show_data:
                    s = str(result)
                    s = s[:120] + "..." if len(s) > 120 else s
                    lines.append(f"  OK  {name:<20} {s}")
                else:
                    lines.append(f"  OK  {name}")
            except RPCError as e:
                lines.append(f"  !!  {name:<28} {e}")
        self.done.emit("\n".join(lines))


class KeyDisplayDialog(_DraggableDialog):
    def __init__(self, parent, key_type, addr, key):
        super().__init__(parent)
        self.setWindowTitle(f"Exported: {key_type}"); self.setMinimumWidth(620)
        self._key = key
        self._addr = addr
        v = QVBoxLayout(self); v.setContentsMargins(18, 18, 18, 18); v.setSpacing(12)
        warn = QLabel("KEEP THIS KEY SECRET.  Anyone with it controls all funds here.")
        warn.setStyleSheet("background:#2d1e1e;color:#f85149;font-weight:700;"
                           "padding:10px;border-radius:6px;border:1px solid #f85149;")
        warn.setWordWrap(True); v.addWidget(warn)

        for label_text, attr_edit, attr_btn, value, echo, reset_label in [
            ("Address:", "_ae", "_bc_addr", addr, QLineEdit.EchoMode.Normal,   "Copy"),
            (f"{key_type}:", "_ke", "_bc_key", key, QLineEdit.EchoMode.Password, "Copy"),
        ]:
            lbl = QLabel(label_text)
            lbl.setStyleSheet("color:#8b949e;font-size:12px;font-weight:700;")
            v.addWidget(lbl)
            row = QHBoxLayout(); row.setSpacing(6)
            edit = QLineEdit(value); edit.setReadOnly(True); edit.setEchoMode(echo)
            setattr(self, attr_edit, edit); row.addWidget(edit)
            btn = QPushButton("Copy"); btn.setMaximumWidth(70)
            btn.clicked.connect(lambda _=False, t=value, b=btn, r=reset_label:
                                self._copy_btn(t, b, r))
            setattr(self, attr_btn, btn); row.addWidget(btn)
            v.addLayout(row)

        h = QHBoxLayout()
        bs = QPushButton("Show"); bs.setCheckable(True)
        bs.toggled.connect(lambda on: self._ke.setEchoMode(
            QLineEdit.EchoMode.Normal if on else QLineEdit.EchoMode.Password))
        h.addWidget(bs)
        self._bc_all = QPushButton("Copy All"); self._bc_all.setObjectName("primary")
        self._bc_all.clicked.connect(
            lambda: self._copy_btn(f"{self._addr} {self._key}", self._bc_all, "Copy All"))
        h.addWidget(self._bc_all)
        h.addStretch()
        bc2 = QPushButton("Close"); bc2.clicked.connect(self.accept); h.addWidget(bc2)
        v.addLayout(h)

    @staticmethod
    def _copy_btn(text: str, btn: QPushButton, reset: str):
        QApplication.clipboard().setText(text)
        btn.setText("Copied!")
        QTimer.singleShot(2000, lambda: _safe_set_text(btn, reset))


class ImportKeyDialog(_DraggableDialog):
    def __init__(self, parent, rpc: BitcoinZRPC):
        super().__init__(parent)
        self._rpc     = rpc
        self._running = False
        self._worker  = None
        self._log_w   = None
        self.setWindowTitle("Import Private Key")
        self.setMinimumWidth(520)
        self._build_ui()

    def _build_ui(self):
        v = QVBoxLayout(self); v.setContentsMargins(20, 20, 20, 20); v.setSpacing(12)

        title = QLabel("Import Private Key")
        title.setStyleSheet("font-size:15px;font-weight:700;"); v.addWidget(title)

        self.lbl_info = QLabel(
            "Transparent key: WIF format  (starts with L, K or 5)\n"
            "Shielded key: spending key  (starts with secret-extended-key...)\n\n"
            "The key will be imported with a blockchain rescan.\n"
            "Do not close this window until the process completes."
        )
        self.lbl_info.setStyleSheet("color:#8b949e;font-size:12px;")
        self.lbl_info.setWordWrap(True); v.addWidget(self.lbl_info)

        self.e_key = QLineEdit()
        self.e_key.setPlaceholderText("Paste private key here")
        v.addWidget(self.e_key)

        self.lbl_status = QLabel("")
        self.lbl_status.setStyleSheet("color:#8b949e;font-size:12px;")
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setVisible(False)
        v.addWidget(self.lbl_status)

        self.prog_bar = QProgressBar(); self.prog_bar.setObjectName("rbar")
        self.prog_bar.setRange(0, 1000); self.prog_bar.setValue(0)
        self.prog_bar.setFormat("Rescanning  0.0%"); self.prog_bar.setFixedHeight(22)
        self.prog_bar.setVisible(False)
        v.addWidget(self.prog_bar)

        self.lbl_block = QLabel("")
        self.lbl_block.setStyleSheet("color:#8b949e;font-size:11px;")
        self.lbl_block.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_block.setVisible(False)
        v.addWidget(self.lbl_block)

        btn_row = QHBoxLayout(); btn_row.setSpacing(8)
        self.btn_import = QPushButton("Import && Scan")
        self.btn_import.setObjectName("primary"); self.btn_import.setMinimumHeight(40)
        self.btn_import.clicked.connect(self._start)
        btn_row.addWidget(self.btn_import)
        self.btn_close = QPushButton("Close"); self.btn_close.setMinimumHeight(40)
        self.btn_close.clicked.connect(self.accept)
        btn_row.addWidget(self.btn_close)
        v.addLayout(btn_row)

    def closeEvent(self, event):
        if self._running: event.ignore()
        else: event.accept()

    def _start(self):
        key = self.e_key.text().strip()
        if not key:
            QMessageBox.warning(self, "Missing", "Please paste a private key."); return

        is_z = key.startswith("secret") or key.startswith("zx") or len(key) > 70
        self._is_z = is_z

        self._running = True
        self.btn_import.setEnabled(False)
        self.btn_import.setObjectName("")
        self.btn_import.setStyle(self.btn_import.style())
        self.btn_close.setEnabled(False)
        self.e_key.setEnabled(False)
        self.e_key.setEchoMode(QLineEdit.EchoMode.Password)

        self.lbl_status.setText("Starting import")
        self.lbl_status.setVisible(True)
        self.prog_bar.setRange(0, 1000)
        self.prog_bar.setValue(0)
        self.prog_bar.setFormat("Rescanning  0.0%")
        self.prog_bar.setVisible(True)
        self.lbl_block.setVisible(True)

        self._log_w = LogTailWorker()
        self._log_w.progress.connect(self._on_log_progress)
        self._log_w.done.connect(
            lambda: self._on_done(
                "Shielded key imported successfully.\nBalances updated." if is_z
                else "Transparent private key imported successfully."
            )
        )
        _track(self._log_w).start()

        self._worker = ImportKeyWorker(self._rpc, key, is_z)
        self._worker.progress.connect(self._on_msg)
        self._worker.error.connect(self._on_error)
        if is_z:
            self._worker.z_key_accepted.connect(self._on_z_accepted)
        else:
            self._worker.done.connect(self._on_done)
        _track(self._worker).start()

    def _on_msg(self, msg: str):
        self.lbl_status.setText(msg.replace("\n", "  "))

    def _on_log_progress(self, frac: float, block: int):
        self.prog_bar.setValue(int(frac * 1000))
        self.prog_bar.setFormat(f"Rescanning  {frac * 100:.1f}%")
        if block:
            self.lbl_block.setText(f"Block {block:,}")

    def _on_z_accepted(self, addr: str):
        self.lbl_status.setText(
            f"Key accepted.  Address: {addr}\nRescanning blockchain" if addr
            else "Key accepted.  Rescanning blockchain"
        )

    def _on_done(self, msg: str):
        self._cleanup()
        self.prog_bar.setValue(1000)
        self.prog_bar.setFormat("Complete!")
        self.lbl_status.setText(msg)
        self.lbl_block.setText("")
        self.btn_import.setEnabled(True)
        self.btn_import.setObjectName("primary")
        self.btn_import.setStyle(self.btn_import.style())
        self.e_key.setEchoMode(QLineEdit.EchoMode.Normal)
        self.e_key.setEnabled(True)
        self.e_key.clear()
        self.btn_close.setEnabled(True)

    def _on_error(self, msg: str):
        self._cleanup()
        self.prog_bar.setRange(0, 1); self.prog_bar.setValue(0)
        self.prog_bar.setFormat("Failed")
        self.lbl_status.setStyleSheet("color:#f85149;font-size:12px;")
        self.lbl_status.setText(f"Error: {msg}")
        self.btn_import.setEnabled(True)
        self.btn_import.setObjectName("primary")
        self.btn_import.setStyle(self.btn_import.style())
        self.e_key.setEchoMode(QLineEdit.EchoMode.Normal)
        self.e_key.setEnabled(True)
        self.btn_close.setEnabled(True)

    def _cleanup(self):
        self._running = False
        if self._log_w:
            self._log_w.stop()
            self._log_w.finished.connect(self._log_w.deleteLater)
            self._log_w = None
        if self._worker:
            try:
                self._worker.progress.disconnect()
                self._worker.error.disconnect()
                if getattr(self, '_is_z', False):
                    self._worker.z_key_accepted.disconnect()
                else:
                    self._worker.done.disconnect()
            except Exception:
                pass
            self._worker.finished.connect(self._worker.deleteLater)
            self._worker = None


def _safe_set_text(widget_ref: "QPushButton | QLabel", text: str) -> None:
    """Call widget.setText(text) only if the underlying C++ object still exists."""
    try:
        widget_ref.setText(text)
    except RuntimeError:
        pass


def mk_card(title, widget):
    c = QFrame(); c.setObjectName("card")
    v = QVBoxLayout(c); v.setContentsMargins(16, 12, 16, 12); v.setSpacing(4)
    lbl = QLabel(title.upper())
    lbl.setStyleSheet("color:#8b949e;font-size:11px;font-weight:700;")
    v.addWidget(lbl); v.addWidget(widget); return c


def slbl(text):
    l = QLabel(text)
    l.setStyleSheet("color:#8b949e;font-size:11px;font-weight:700;")
    return l


def mk_tbl(headers):
    t = QTableWidget(0, len(headers))
    t.setHorizontalHeaderLabels(headers)
    t.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    t.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    t.verticalHeader().setVisible(False)
    t.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
    t.viewport().setFocusPolicy(Qt.FocusPolicy.ClickFocus)

    hdr = t.horizontalHeader()
    hdr.setHighlightSections(False)
    hdr.setStyleSheet(
        "QHeaderView::section{"
        "background:#21262d;color:#8b949e;padding:8px 12px;"
        "border:none;font-size:12px;font-weight:700;}"
        "QHeaderView::section:hover{"
        "background:#2d333b;color:#e6edf3;font-size:12px;font-weight:700;}"
    )
    return t


class StartupScreen(QWidget):
    ready = Signal()
    raise_window = Signal()

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__()
        self.setWindowTitle("ZSend Wallet"); self.resize(520, 260)
        self.setStyleSheet("background:#1c2128;color:#e6edf3;font-family:'Segoe UI';")
        self._rpc = rpc
        v = QVBoxLayout(self); v.setContentsMargins(36, 32, 36, 32); v.setSpacing(14)
        logo = QLabel("ZSend Wallet")
        logo.setStyleSheet("font-size:22px;font-weight:900;color:#f7a32c;")
        logo.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(logo)
        self.lbl = QLabel("Checking ZcashParams")
        self.lbl.setStyleSheet("color:#8b949e;font-size:13px;")
        self.lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(self.lbl)
        self.bar = QProgressBar(); self.bar.setObjectName("sbar")
        self.bar.setRange(0, 0); self.bar.setFixedHeight(16); v.addWidget(self.bar)
        self.lbl_size = QLabel("")
        self.lbl_size.setStyleSheet("color:#8b949e;font-size:11px;")
        self.lbl_size.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(self.lbl_size)

        self._pw = ParamsWorker()
        self._pw.status.connect(self.lbl.setText)
        self._pw.progress.connect(self._on_params_progress)
        self._pw.done.connect(self._params_ok)
        self._pw.failed.connect(self._params_fail)
        _track(self._pw).start()

    def _on_params_progress(self, done: int, total: int):
        self.bar.setRange(0, total)
        self.bar.setValue(done)
        mb_done  = done  / 1_048_576
        mb_total = total / 1_048_576
        self.lbl_size.setText(f"{mb_done:.1f} MB / {mb_total:.0f} MB")

    def _params_ok(self):
        self.bar.setRange(0, 0)
        self.lbl_size.setText("")
        self.lbl.setText("Starting node")
        self._nw = NodeStartWorker(self._rpc)
        self._nw.status.connect(self.lbl.setText)
        self._nw.ready.connect(self._ok)
        self._nw.failed.connect(self._fail)
        _track(self._nw).start()

    def _params_fail(self, msg: str):
        self.bar.setRange(0, 1); self.bar.setValue(0)
        self.lbl.setText(f"Error: {msg.splitlines()[0]}")
        QMessageBox.critical(None, "ZcashParams Error", msg)
        QTimer.singleShot(500, self.ready.emit)

    def _ok(self):
        self.bar.setRange(0, 1); self.bar.setValue(1)
        self.lbl.setText("Node is ready!")
        self.lbl_size.setText("")
        QTimer.singleShot(500, self.ready.emit)

    def _fail(self, msg):
        self.bar.setRange(0, 1); self.bar.setValue(0)
        self.lbl.setText(f"Warning: {msg.splitlines()[0]}")
        if len(msg.splitlines()) > 1:
            QMessageBox.warning(None, "Node Connection Failed", msg)
        QTimer.singleShot(2000, self.ready.emit)


class _AddrBalanceDelegate(QStyledItemDelegate):

    _PAD = 10

    def paint(self, painter, option, index):
        self.initStyleOption(option, index)
        style = option.widget.style() if option.widget else QApplication.style()
        style.drawPrimitive(QStyle.PrimitiveElement.PE_PanelItemViewItem, option, painter, option.widget)

        text = index.data(Qt.ItemDataRole.DisplayRole) or ""
        parts = text.rsplit("   ", 1)
        addr = parts[0].strip()
        bal  = parts[1].strip() if len(parts) == 2 else ""

        rect: QRect = option.rect.adjusted(self._PAD, 0, -self._PAD, 0)
        color = option.palette.highlightedText().color() \
            if option.state & QStyle.StateFlag.State_Selected \
            else option.palette.text().color()

        painter.save()
        painter.setPen(color)

        bal_w = painter.fontMetrics().horizontalAdvance(bal) if bal else 0
        bal_rect  = QRect(rect.right() - bal_w, rect.top(), bal_w, rect.height())
        addr_rect = QRect(rect.left(), rect.top(), rect.width() - bal_w - self._PAD, rect.height())

        painter.drawText(addr_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, addr)
        if bal:
            bal_color = color
            bal_color.setAlphaF(0.65)
            painter.setPen(bal_color)
            painter.drawText(bal_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, bal)
        painter.restore()

    def sizeHint(self, option, index):
        sh = super().sizeHint(option, index)
        from PySide6.QtCore import QSize
        return QSize(sh.width(), max(sh.height(), 28))


class _FromCombo(QComboBox):

    _PAD = 6

    def paintEvent(self, event):
        opt = QStyleOptionComboBox()
        self.initStyleOption(opt)
        p = QPainter(self)
        style = self.style()

        opt.currentText = ""
        style.drawComplexControl(QStyle.ComplexControl.CC_ComboBox, opt, p, self)

        text = self.currentText()
        parts = text.rsplit("   ", 1)
        addr = parts[0].strip() if parts else text
        bal  = parts[1].strip() if len(parts) == 2 else ""

        rect = style.subControlRect(
            QStyle.ComplexControl.CC_ComboBox, opt, QStyle.SubControl.SC_ComboBoxEditField, self
        ).adjusted(self._PAD, 0, -self._PAD, 0)

        fm = p.fontMetrics()
        p.setPen(self.palette().text().color())

        if bal:
            bal_w = fm.horizontalAdvance(bal)
            addr_rect = QRect(rect.left(), rect.top(),
                              rect.width() - bal_w - self._PAD, rect.height())
            bal_rect  = QRect(rect.right() - bal_w, rect.top(), bal_w, rect.height())

            addr_clipped = fm.elidedText(addr, Qt.TextElideMode.ElideMiddle, addr_rect.width())
            p.drawText(addr_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, addr_clipped)

            color = self.palette().text().color()
            color.setAlphaF(0.65)
            p.setPen(color)
            p.drawText(bal_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, bal)
        else:
            p.drawText(rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                       fm.elidedText(text, Qt.TextElideMode.ElideMiddle, rect.width()))
        p.end()


class TxDetailDialog(_DraggableDialog):

    def __init__(self, parent, tx: dict, rpc: BitcoinZRPC):
        super().__init__(parent)
        self.setWindowTitle("Transaction Details")
        self.setMinimumSize(720, 650)
        self.resize(720, 650)
        self._rpc      = rpc
        self._tx       = tx
        self._txid     = tx.get("txid", "")
        self._confirms = 0
        self._build_ui()
        self._load()

        if parent:
            pg = parent.frameGeometry()
            self.move(
                pg.x() + (pg.width()  - self.width())  // 2,
                pg.y() + (pg.height() - self.height()) // 2,
            )

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._auto_refresh)
        self._timer.start(30_000)

    def _auto_refresh(self):
        if self._confirms >= 6:
            self._timer.stop()
            return
        self._load()

    def closeEvent(self, event):
        self._timer.stop()
        super().closeEvent(event)

    def _build_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(18, 16, 18, 16); root.setSpacing(10)

        title = QLabel("Transaction Details")
        title.setStyleSheet("font-size:15px;font-weight:700;")
        root.addWidget(title)

        self._scroll_area = QFrame()
        self._scroll_area.setObjectName("card")
        self._content_layout = QVBoxLayout(self._scroll_area)
        self._content_layout.setContentsMargins(16, 14, 16, 14)
        self._content_layout.setSpacing(0)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(self._scroll_area)
        root.addWidget(scroll, 1)

        btns = QHBoxLayout()
        self._btn_copy = QPushButton("Copy TxID"); self._btn_copy.setObjectName("primary")
        self._btn_copy.clicked.connect(self._copy_txid)
        btns.addWidget(self._btn_copy); btns.addStretch()
        bclose = QPushButton("Close"); bclose.clicked.connect(self.accept)
        btns.addWidget(bclose)
        root.addLayout(btns)

    def _copy_txid(self):
        QApplication.clipboard().setText(self._txid)
        self._btn_copy.setText("Copied!")
        QTimer.singleShot(2000, lambda: _safe_set_text(self._btn_copy, "Copy TxID"))

    def _sep(self):
        f = QFrame()
        f.setStyleSheet("background:#30363d;")
        f.setFixedHeight(1)
        self._content_layout.addWidget(f)
        self._content_layout.addSpacing(6)

    def _row(self, label: str, value: str, selectable: bool = False, color: str = "",
             mono: bool = False, wrap: bool = True):
        h = QHBoxLayout(); h.setSpacing(8)
        lbl = QLabel(label)
        lbl.setStyleSheet("color:#8b949e;font-size:12px;min-width:130px;")
        lbl.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        lbl.setFixedWidth(130)
        h.addWidget(lbl)
        val = QLabel(value)
        val.setWordWrap(wrap)
        style = "font-size:12px;"
        if color: style += f"color:{color};"
        if mono:  style += "font-family:Consolas,'Courier New',monospace;"
        val.setStyleSheet(style)
        if selectable:
            val.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        h.addWidget(val, 1)
        self._content_layout.addLayout(h)
        self._content_layout.addSpacing(4)

    def _section(self, title: str):
        lbl = QLabel(title)
        lbl.setStyleSheet(
            "color:#8b949e;font-size:11px;font-weight:700;"
            "background:#21262d;border-radius:3px;padding:3px 8px;"
        )
        self._content_layout.addSpacing(6)
        self._content_layout.addWidget(lbl)
        self._content_layout.addSpacing(6)

    def _clear(self):
        while self._content_layout.count():
            item = self._content_layout.takeAt(0)
            if item.widget(): item.widget().deleteLater()
            elif item.layout():
                while item.layout().count():
                    sub = item.layout().takeAt(0)
                    if sub.widget(): sub.widget().deleteLater()

    @staticmethod
    def _addr_row_parts(addr: str) -> tuple[str, str]:
        pfx  = "🛡 " if _is_z_addr(addr) else ""
        disp = (addr[:18] + "…" + addr[-10:]) if len(addr) > 30 else addr
        return pfx, disp

    def _load(self):
        self._clear()
        tx   = self._tx
        txid = self._txid

        self._row("TxID:", txid, selectable=True, mono=True)
        self._sep()

        try:
            full = self._rpc.getTransaction(txid)
            tx   = full
        except RPCError:
            full = {}

        confirms = int(tx.get("confirmations", 0))
        self._confirms = confirms
        if confirms < 0:
            status_txt = "Failed  ✗"; sc = "#f85149"
        elif confirms == 0:
            status_txt = "Pending  ⏳  0 confirmations (not yet in a block)"; sc = "#f7a32c"
        elif confirms < 6:
            status_txt = f"Confirming…  {confirms}/6 confirmations"; sc = "#58a6ff"
        else:
            status_txt = f"Confirmed  ✓  {confirms:,} confirmations"; sc = "#238636"
        self._row("Status:", status_txt, color=sc)

        ts_block    = tx.get("blocktime")
        ts_received = tx.get("timereceived") or tx.get("time")
        if ts_block:
            self._row("Date:", fmt_ts(ts_block))
        elif ts_received:
            self._row("Date:", fmt_ts(ts_received))

        details = full.get("details") or []

        cat = self._tx.get("category", "")
        if not cat and details:
            cat = details[0].get("category", "")
        self._row("Type:", cat.capitalize() if cat else "—")

        amt = float(self._tx.get("amount", tx.get("amount", 0)))
        sign = "+" if amt >= 0 else ""
        self._row("Amount:", f"{sign}{fmt_btcz(amt)} BTCZ",
                  color="#238636" if amt >= 0 else "#f85149")

        fee = tx.get("fee")
        if fee is not None and fee != 0:
            self._row("Fee:", f"{fmt_btcz(abs(float(fee)))} BTCZ")

        addr = self._tx.get("address", "")

        try:
            raw        = self._rpc.getRawTransaction(txid)
            vouts      = raw.get("vout", [])
            size       = raw.get("size", "—")
            locktime   = raw.get("locktime")
            has_shielded_in  = bool(raw.get("vJoinSplit") or raw.get("vShieldedSpend"))
            has_shielded_out = bool(raw.get("vJoinSplit") or raw.get("vShieldedOutput"))
        except RPCError:
            raw = {}; vouts = []; size = "—"; locktime = None
            has_shielded_in = False; has_shielded_out = False

        sends    = [d for d in details if d.get("category") == "send"]
        receives = [d for d in details if d.get("category") == "receive"]

        self._sep()
        self._section("From")
        if sends:
            for d in sends:
                d_addr = d.get("address", "")
                d_amt  = abs(float(d.get("amount", 0)))
                if d_addr:
                    pfx, disp = self._addr_row_parts(d_addr)
                    self._row(f"{pfx}From:", f"{disp}   -{fmt_btcz(d_amt)} BTCZ",
                              selectable=True, mono=True, color="#f85149")
                else:
                    self._row("🛡 From:", f"(shielded — sender hidden)   -{fmt_btcz(d_amt)} BTCZ",
                              color="#f85149")
        elif has_shielded_in:
            self._row("🛡 From:", "(shielded z-address — sender hidden)", color="#8b949e")
        elif addr:
            pfx, disp = self._addr_row_parts(addr)
            self._row(f"{pfx}From:", disp, selectable=True, mono=True, color="#8b949e")
        else:
            self._row("From:", "—", color="#8b949e")

        self._section("To")
        if receives:
            for d in receives:
                d_addr = d.get("address", "")
                d_amt  = float(d.get("amount", 0))
                if d_addr:
                    pfx, disp = self._addr_row_parts(d_addr)
                    self._row(f"{pfx}To:", f"{disp}   +{fmt_btcz(d_amt)} BTCZ",
                              selectable=True, mono=True, color="#238636")
                else:
                    self._row("🛡 To:", f"(shielded — recipient hidden)   +{fmt_btcz(d_amt)} BTCZ",
                              color="#238636")
        elif has_shielded_out:
            self._row("🛡 To:", "(shielded z-address — recipient hidden)", color="#238636")
        elif addr:
            pfx, disp = self._addr_row_parts(addr)
            self._row(f"{pfx}To:", disp, selectable=True, mono=True, color="#238636")
        else:
            self._row("To:", "—", color="#8b949e")

        blockhash = tx.get("blockhash", "")
        if blockhash:
            self._sep()
            blockheight = tx.get("blockheight") or tx.get("blockindex", "")
            if blockheight:
                self._row("Block Height:", f"{int(blockheight):,}")
            blocktime = tx.get("blocktime")
            if blocktime:
                self._row("Block Time:", fmt_ts(blocktime))
            self._row("Block Hash:", blockhash, selectable=True, mono=True)

        if raw:
            self._sep()
            self._row("Size:", f"{size} bytes" if isinstance(size, int) else str(size))
            if locktime is not None:
                self._row("LockTime:", f"{locktime} (height)" if locktime < 500_000_000
                          else fmt_ts(locktime))

            if vouts:
                self._sep()
                self._section(f"Outputs ({len(vouts)})")
                for out in vouts:
                    sc_out = out.get("scriptPubKey", {})
                    addrs  = sc_out.get("addresses") or sc_out.get("address", [])
                    if isinstance(addrs, str): addrs = [addrs]
                    out_amt = float(out.get("value", 0))
                    for a in addrs:
                        pfx, _ = self._addr_row_parts(a)
                        self._row(f"{pfx}→", f"{a}   {fmt_btcz(out_amt)} BTCZ",
                                  selectable=True, mono=True, color="#e6edf3")

        self._content_layout.addStretch()


class AboutDialog(_DraggableDialog):
    def __init__(self, parent, rpc: BitcoinZRPC):
        super().__init__(parent)
        self.setWindowTitle("About ZSend Wallet")
        self.setMinimumWidth(520)
        self._rpc = rpc
        self._build_ui()
        self._ver_worker = _NodeVersionWorker(rpc)
        self._ver_worker.done.connect(self._on_version)
        _track(self._ver_worker).start()

    @staticmethod
    def _div():
        f = QFrame(); f.setStyleSheet("background:#30363d;"); f.setFixedHeight(1)
        return f

    def _build_ui(self):
        v = QVBoxLayout(self); v.setContentsMargins(24, 24, 24, 24); v.setSpacing(14)

        title = QLabel("ZSend Wallet")
        title.setStyleSheet("font-size:22px;font-weight:900;color:#f7a32c;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(title)

        ver_lbl = QLabel(f"Version {wallet_version}  ·  Author: <b>Zalpader</b>")
        ver_lbl.setStyleSheet("color:#8b949e;font-size:12px;")
        ver_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(ver_lbl)

        v.addWidget(self._div())

        desc = QLabel(
            "A desktop wallet for the <b>BitcoinZ</b> (BTCZ) cryptocurrency.<br>"
            "Supports transparent (t-address) and shielded (z-address) transactions<br>"
            "with full privacy using the Sapling protocol."
        )
        desc.setStyleSheet("color:#c9d1d9;font-size:12px;line-height:160%;")
        desc.setWordWrap(True); desc.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(desc)

        v.addWidget(self._div())

        links = QLabel(
            '<a href="https://github.com/zalpader/ZSend_Wallet" '
            'style="color:#58a6ff;text-decoration:none;">ZSend Wallet</a>'
            '&nbsp;&nbsp;·&nbsp;&nbsp;'
            '<a href="https://getbtcz.com" '
            'style="color:#58a6ff;text-decoration:none;">getbtcz.com</a>'
        )
        links.setStyleSheet("font-size:12px;")
        links.setAlignment(Qt.AlignmentFlag.AlignCenter)
        links.setOpenExternalLinks(True)
        v.addWidget(links)

        v.addWidget(self._div())

        self.lbl_node = QLabel("Node version: fetching…")
        self.lbl_node.setStyleSheet("color:#e6edf3;font-size:12px;")
        self.lbl_node.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(self.lbl_node)

        v.addWidget(self._div())

        lic = QLabel(
            "<b>Open source libraries used:</b><br>"
            '• <b>Python 3</b> — '
            '<a href="https://docs.python.org/3/license.html" '
            'style="color:#58a6ff;text-decoration:none;">PSF License</a><br>'
            '• <b>PySide6</b> — '
            '<a href="https://doc.qt.io/qtforpython-6/" '
            'style="color:#58a6ff;text-decoration:none;">Qt for Python</a> '
            '(<a href="https://www.gnu.org/licenses/lgpl-3.0.en.html" '
            'style="color:#58a6ff;text-decoration:none;">LGPL v3</a>)<br>'
            '• <b>requests</b> — '
            '<a href="https://github.com/psf/requests/blob/main/LICENSE" '
            'style="color:#58a6ff;text-decoration:none;">Apache License 2.0</a><br>'
            '• <b>BitcoinZ Core</b> — '
            '<a href="https://github.com/btcz/bitcoinz/blob/master/COPYING" '
            'style="color:#58a6ff;text-decoration:none;">MIT License</a> '
            '(<a href="https://github.com/btcz/bitcoinz" '
            'style="color:#58a6ff;text-decoration:none;">source code</a>)'
        )
        lic.setStyleSheet("color:#8b949e;font-size:11px;line-height:160%;")
        lic.setWordWrap(True)
        lic.setOpenExternalLinks(True)
        v.addWidget(lic)

        h = QHBoxLayout()
        h.addStretch()
        bc = QPushButton("Close"); bc.setObjectName("primary")
        bc.clicked.connect(self.accept); h.addWidget(bc)
        v.addLayout(h)

    def _on_version(self, info: dict):
        version = info.get("version", "")
        build   = info.get("build", "")
        subver  = info.get("subversion", "")
        if version or build:
            parts = []
            if build:   parts.append(f"Build: {build}")
            if version: parts.append(f"Protocol: {version}")
            if subver:  parts.append(f"Agent: {subver.strip('/')}")
            self.lbl_node.setText("Node  ·  " + "  ·  ".join(parts))
        else:
            self.lbl_node.setText("Node version: not connected")
        self._ver_worker = None

    def closeEvent(self, event):
        if self._ver_worker is not None:
            try: self._ver_worker.done.disconnect()
            except Exception: pass
            self._ver_worker = None
        super().closeEvent(event)


class _NodeVersionWorker(QThread):
    done = Signal(object)

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__(); self.rpc = rpc

    def run(self):
        try:
            info = self.rpc.getInfo()
            self.done.emit({
                "version":    str(info.get("version", "")),
                "build":      str(info.get("build", "")),
                "subversion": str(info.get("subversion", "")),
            })
        except Exception:
            self.done.emit({})


class _CenteredTabWidget(QTabWidget):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.tabBar().setExpanding(False)
        self._center_timer = QTimer(self)
        self._center_timer.setSingleShot(True)
        self._center_timer.setInterval(60)
        self._center_timer.timeout.connect(self._apply_centering)

    def _apply_centering(self):
        tb = self.tabBar()
        if tb.count() == 0:
            return
        total_w = sum(tb.tabRect(i).width() for i in range(tb.count()))
        offset  = max(0, (self.width() - total_w) // 2)
        self.setStyleSheet(f"QTabWidget::tab-bar {{ left: {offset}px; }}")

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._center_timer.start()

    def showEvent(self, event):
        super().showEvent(event)
        self._center_timer.start()


class MainWindow(QMainWindow):

    raise_window = Signal()

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__()
        self.setWindowTitle("ZSend Wallet")
        self.resize(750, 810); self.setMinimumSize(750, 810)
        self.rpc = rpc
        self._data: dict            = {}
        self._cur_blocks: int       = 0
        self._tx_desc: bool         = True
        self._cached_txs: list      = []
        self._addr_balances: dict   = {}
        self._max_mode: bool        = False
        self._refresh_running: bool = False
        self._had_balance: bool     = False
        self._tx_cache_key: str     = ""
        self._threads: list         = []
        self._t_sort_mode: str      = 'balance'
        self._z_sort_mode: str      = 'balance'

        self.raise_window.connect(self._bring_to_front)
        self._build_menu(); self._build_ui(); self._build_sb()

        self._timer = QTimer(self); self._timer.timeout.connect(self.refresh)
        self._timer.start(30_000)
        self.refresh()

    def _bring_to_front(self):
        self.setWindowState(Qt.WindowState.WindowActive)
        self.show(); self.raise_(); self.activateWindow()

    def closeEvent(self, event):
        self._timer.stop()
        for w in list(_RUNNING_WORKERS):
            if hasattr(w, 'stop'):
                try:
                    w.stop()
                except Exception:
                    pass
        for w in list(_RUNNING_WORKERS):
            try:
                w.wait(1500)
            except Exception:
                pass
        super().closeEvent(event)

    def _build_menu(self):
        mb = self.menuBar()
        wm = mb.addMenu("Wallet")
        self._act(wm, "Refresh",           self.refresh,      "F5")
        wm.addSeparator()
        self._act_import = self._act(wm, "Import Private Key", self._import_key)
        self._act_import.setEnabled(False)
        wm.addSeparator()
        self._act(wm, "Stop Node && Exit",  self._quit_and_stop)
        self._act(wm, "Exit",               self.close)
        sm = mb.addMenu("Settings")
        self._act(sm, "RPC Connection", self._open_cfg)
        hm = mb.addMenu("Help")
        self._act(hm, "Full Diagnostics", self._open_diag)
        hm.addSeparator()
        self._act(hm, "About", self._open_about)

    def _act(self, menu, text, slot, sc=None):
        a = QAction(text, self); a.triggered.connect(slot)
        if sc: a.setShortcut(sc)
        menu.addAction(a)
        return a

    def _build_ui(self):
        cw = QWidget(); self.setCentralWidget(cw)
        root = QVBoxLayout(cw); root.setContentsMargins(16, 12, 16, 12); root.setSpacing(10)

        cr = QHBoxLayout(); cr.setSpacing(12)
        self.lbl_transp = QLabel("-")
        self.lbl_transp.setStyleSheet("color:#1f6feb;font-size:19px;font-weight:700;")
        self.lbl_priv   = QLabel("-")
        self.lbl_priv.setStyleSheet("color:#238636;font-size:19px;font-weight:700;")
        self.lbl_total  = QLabel("-")
        self.lbl_total.setStyleSheet("color:#f7a32c;font-size:19px;font-weight:700;")
        cr.addWidget(mk_card("Transparent",          self.lbl_transp))
        cr.addWidget(mk_card("Shielded (Private)",   self.lbl_priv))
        cr.addWidget(mk_card("Total Balance (BTCZ)", self.lbl_total))
        root.addLayout(cr)

        self.tabs = _CenteredTabWidget()
        self.tabs.addTab(self._tab_t(),    "T-Addresses")
        self.tabs.addTab(self._tab_z(),    "Z-Addresses")
        self.tabs.addTab(self._tab_send(), "Send")
        self.tabs.addTab(self._tab_tx(),   "Transactions")
        root.addWidget(self.tabs)

    def _tab_t(self):
        w = QWidget(); v = QVBoxLayout(w); v.setContentsMargins(12, 12, 12, 12); v.setSpacing(8)
        h = QHBoxLayout()
        lbl = QLabel("Transparent addresses (t-addresses)")
        lbl.setStyleSheet("font-weight:600;font-size:14px;"); h.addWidget(lbl); h.addStretch()
        b1 = QPushButton("New T-Address")
        b1.setStyleSheet(
            "QPushButton{background:#1f6feb;color:#fff;border:none;border-radius:6px;"
            "padding:8px 18px;font-weight:700;}"
            "QPushButton:hover{background:#388bfd;}"
            "QPushButton:pressed{background:#1158c7;}")
        b1.clicked.connect(self._new_t); h.addWidget(b1)
        v.addLayout(h)
        self.tbl_t = mk_tbl(["Address", "Balance (BTCZ)"])
        self.tbl_t.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.tbl_t.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.tbl_t.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tbl_t.customContextMenuRequested.connect(self._t_ctx)
        self.tbl_t.horizontalHeader().sectionClicked.connect(self._t_header_click)
        v.addWidget(self.tbl_t); return w

    def _tab_z(self):
        w = QWidget(); v = QVBoxLayout(w); v.setContentsMargins(12, 12, 12, 12); v.setSpacing(8)
        h = QHBoxLayout()
        lbl = QLabel("Shielded z-addresses (fully private)")
        lbl.setStyleSheet("font-weight:600;font-size:14px;"); h.addWidget(lbl); h.addStretch()
        b1 = QPushButton("New Z-Address"); b1.setObjectName("shield")
        b1.clicked.connect(self._new_z); h.addWidget(b1)
        v.addLayout(h)
        self.tbl_z = mk_tbl(["Z-Address", "Balance (BTCZ)"])
        self.tbl_z.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.tbl_z.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.tbl_z.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tbl_z.customContextMenuRequested.connect(self._z_ctx)
        self.tbl_z.horizontalHeader().sectionClicked.connect(self._z_header_click)
        v.addWidget(self.tbl_z); return w

    def _t_header_click(self, col: int):
        self._t_sort_mode = 'name' if col == 0 else 'balance'
        if self._data: self._fill_t_table(self._data.get('t_balances', {}))

    def _z_header_click(self, col: int):
        self._z_sort_mode = 'name' if col == 0 else 'balance'
        if self._data: self._fill_z_table(self._data.get('z_balances', {}))

    def _update_header_indicators(self, tbl, sort_mode: str, base_label: str):
        if sort_mode == 'name':
            a, b = f"↑ {base_label} (A→Z)", "Balance (BTCZ)"
        else:
            a, b = base_label, "↓ Balance (BTCZ)"
        tbl.setHorizontalHeaderItem(0, QTableWidgetItem(a))
        tbl.setHorizontalHeaderItem(1, QTableWidgetItem(b))

    def _update_t_header_indicators(self):
        self._update_header_indicators(self.tbl_t, self._t_sort_mode, "Address")

    def _update_z_header_indicators(self):
        self._update_header_indicators(self.tbl_z, self._z_sort_mode, "Z-Address")

    def _fill_addr_table(self, tbl, bal_dict: dict, sort_mode: str, update_fn):
        sorted_items = _sort_addr_items(list(bal_dict.items()), sort_mode)

        existing: dict[str, int] = {}
        for r in range(tbl.rowCount()):
            it = tbl.item(r, 0)
            if it:
                existing[it.text()] = r

        new_addrs = [addr for addr, _ in sorted_items]
        new_set   = set(new_addrs)

        tbl.setUpdatesEnabled(False)

        for addr in list(existing.keys()):
            if addr not in new_set:
                tbl.removeRow(existing[addr])
                existing = {
                    tbl.item(r, 0).text(): r
                    for r in range(tbl.rowCount())
                    if tbl.item(r, 0)
                }

        for target_row, (addr, bal) in enumerate(sorted_items):
            bal_str = fmt_btcz(float(bal))
            if addr in existing:
                cur_row = existing[addr]
                if cur_row != target_row:
                    tbl.insertRow(target_row)
                    for col in range(tbl.columnCount()):
                        old_it = tbl.item(cur_row + 1, col)
                        tbl.setItem(target_row, col,
                                    QTableWidgetItem(old_it) if old_it else QTableWidgetItem())
                    tbl.removeRow(cur_row + 1)
                    existing = {
                        tbl.item(r, 0).text(): r
                        for r in range(tbl.rowCount())
                        if tbl.item(r, 0)
                    }
                bal_item = tbl.item(target_row, 1)
                if bal_item and bal_item.text() != bal_str:
                    bal_item.setText(bal_str)
            else:
                tbl.insertRow(target_row)
                tbl.setItem(target_row, 0, QTableWidgetItem(addr))
                it = QTableWidgetItem(bal_str)
                it.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                tbl.setItem(target_row, 1, it)
                existing[addr] = target_row

        tbl.setUpdatesEnabled(True)
        update_fn()

    def _fill_t_table(self, t_bal: dict):
        self._fill_addr_table(self.tbl_t, t_bal, self._t_sort_mode, self._update_t_header_indicators)

    def _fill_z_table(self, z_bal: dict):
        self._fill_addr_table(self.tbl_z, z_bal, self._z_sort_mode, self._update_z_header_indicators)

    def _fill_combo_from(self, t_addrs, z_addrs, t_bal: dict, z_bal: dict, prev_data):
        t_items = _sort_addr_items([(a, t_bal.get(a, 0.0)) for a in t_addrs], 'balance')
        z_items = _sort_addr_items([(a, z_bal.get(a, 0.0)) for a in z_addrs], 'balance')

        self.combo_from.blockSignals(True)
        self.combo_from.clear()
        for addr, bal in t_items + z_items:
            self.combo_from.addItem(f"{_fmt_addr(addr)}   {fmt_btcz(bal)} BTCZ", userData=addr)
        self.combo_from.blockSignals(False)

        for i in range(self.combo_from.count()):
            if self.combo_from.itemData(i) == prev_data:
                self.combo_from.setCurrentIndex(i); break

    def _tab_send(self):
        w = QWidget(); outer = QHBoxLayout(w); outer.setContentsMargins(0, 0, 0, 0)
        p = QWidget(); p.setMinimumWidth(560); p.setMaximumWidth(820)
        v = QVBoxLayout(p); v.setContentsMargins(32, 20, 32, 20); v.setSpacing(10)

        ttl = QLabel("Send BTCZ"); ttl.setStyleSheet("font-size:18px;font-weight:700;")
        v.addWidget(ttl)

        v.addWidget(slbl("FROM ADDRESS"))
        self.combo_from = _FromCombo()
        self.combo_from.setItemDelegate(_AddrBalanceDelegate(self.combo_from))
        self.combo_from.setFixedHeight(38)
        self.combo_from.currentIndexChanged.connect(self._on_from_changed)
        v.addWidget(self.combo_from)

        v.addWidget(slbl("TO ADDRESS"))
        self.e_to = QLineEdit(); self.e_to.setPlaceholderText("t1... or zs1...")
        self.e_to.setFixedHeight(38)
        self.e_to.textChanged.connect(self._validate_to_addr)
        v.addWidget(self.e_to)

        v.addWidget(slbl("AMOUNT (BTCZ)"))
        amt_row = QHBoxLayout(); amt_row.setSpacing(6)
        self.spin_amt = QDoubleSpinBox()
        self.spin_amt.setDecimals(8); self.spin_amt.setMaximum(21_000_000)
        self.spin_amt.setMinimum(0); self.spin_amt.setValue(0)
        self.spin_amt.setSpecialValueText("0")
        self.spin_amt.setSuffix("  BTCZ")
        self.spin_amt.setFixedHeight(38)
        self.spin_amt.valueChanged.connect(self._on_amt_changed)
        amt_row.addWidget(self.spin_amt)
        self.btn_max = QPushButton("MAX"); self.btn_max.setMaximumWidth(54)
        self.btn_max.setFixedHeight(38)
        self.btn_max.setCheckable(True)
        self.btn_max.setStyleSheet(
            "QPushButton{background:#21262d;color:#8b949e;border:1px solid #30363d;"
            "border-radius:6px;padding:6px 8px;font-weight:700;font-size:12px;}"
            "QPushButton:hover{background:#2d2d1a;color:#f7a32c;border-color:#f7a32c;}"
            "QPushButton:checked{background:#2d1a00;color:#f7a32c;border:2px solid #f7a32c;}"
            "QPushButton:checked:hover{background:#3d2600;}")
        self.btn_max.toggled.connect(self._toggle_max)
        amt_row.addWidget(self.btn_max); v.addLayout(amt_row)

        v.addWidget(slbl("NETWORK FEE (BTCZ)"))
        self.spin_fee = QDoubleSpinBox()
        self.spin_fee.setDecimals(8); self.spin_fee.setMaximum(1.0)
        self.spin_fee.setMinimum(0.00000001); self.spin_fee.setValue(0.00001)
        self.spin_fee.setSingleStep(0.000005)
        self.spin_fee.setSuffix("  BTCZ")
        self.spin_fee.setFixedHeight(38)
        self.spin_fee.valueChanged.connect(self._on_fee_changed)
        v.addWidget(self.spin_fee)

        v.addWidget(slbl("MEMO  (optional - shielded only)"))
        self.e_memo = QLineEdit(); self.e_memo.setPlaceholderText("Text memo")
        self.e_memo.setFixedHeight(38)
        v.addWidget(self.e_memo)

        self.send_summary = QFrame()
        self.send_summary.setStyleSheet(
            "QFrame{background:#1c2128;border:1px solid #30363d;border-radius:8px;}")
        sg = QVBoxLayout(self.send_summary); sg.setContentsMargins(14, 10, 14, 10); sg.setSpacing(4)

        def _srow(label, attr):
            h = QHBoxLayout()
            lbl = QLabel(label); lbl.setStyleSheet("color:#8b949e;font-size:12px;")
            h.addWidget(lbl); h.addStretch()
            val = QLabel("-"); val.setStyleSheet("color:#e6edf3;font-size:12px;font-weight:600;")
            h.addWidget(val); sg.addLayout(h); setattr(self, attr, val)

        _srow("Recipient receives:", "lbl_sum_recv")
        _srow("Network fee:",        "lbl_sum_fee")
        div2 = QFrame(); div2.setStyleSheet("background:#30363d;"); div2.setFixedHeight(1)
        sg.addWidget(div2)
        h_tot = QHBoxLayout()
        lt = QLabel("Total amount:"); lt.setStyleSheet("color:#8b949e;font-size:12px;font-weight:700;")
        h_tot.addWidget(lt); h_tot.addStretch()
        self.lbl_sum_total = QLabel("-")
        self.lbl_sum_total.setStyleSheet("color:#f7a32c;font-size:13px;font-weight:700;")
        h_tot.addWidget(self.lbl_sum_total); sg.addLayout(h_tot)
        v.addWidget(self.send_summary)

        self.btn_send = QPushButton("SEND"); self.btn_send.setObjectName("primary")
        self.btn_send.setMinimumHeight(44); self.btn_send.clicked.connect(self._do_send)
        self.btn_send.setEnabled(False)
        v.addWidget(self.btn_send)

        self.lbl_op = QLabel(""); self.lbl_op.setWordWrap(True)
        self.lbl_op.setStyleSheet("color:#8b949e;font-size:12px;padding-top:2px;")
        v.addWidget(self.lbl_op)
        v.addStretch()
        outer.addStretch(); outer.addWidget(p); outer.addStretch()
        return w

    def _tab_tx(self):
        w = QWidget(); v = QVBoxLayout(w); v.setContentsMargins(12, 12, 12, 12); v.setSpacing(8)
        h = QHBoxLayout()
        lbl = QLabel("Transactions"); lbl.setStyleSheet("font-weight:600;font-size:14px;")
        h.addWidget(lbl); h.addStretch()
        self.btn_sort = QPushButton("↓ Newest first")
        self.btn_sort.setStyleSheet(
            "QPushButton{background:#1c2128;color:#f7a32c;border:1px solid #f7a32c;"
            "border-radius:6px;padding:6px 14px;font-weight:700;}"
            "QPushButton:hover{background:#2d1a00;}")
        self.btn_sort.clicked.connect(self._toggle_sort); h.addWidget(self.btn_sort)
        btn_ref = QPushButton("Refresh"); btn_ref.clicked.connect(self.refresh); h.addWidget(btn_ref)
        v.addLayout(h)

        self.tbl_tx = mk_tbl(["Date", "Address", "Status", "Amount (BTCZ)"])
        hdr = self.tbl_tx.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        hdr.sectionClicked.connect(lambda c: self._toggle_sort() if c == 0 else None)
        self.tbl_tx.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tbl_tx.customContextMenuRequested.connect(self._tx_ctx)
        self.tbl_tx.doubleClicked.connect(self._tx_double_click)
        v.addWidget(self.tbl_tx); return w

    def _build_sb(self):
        sb = self.statusBar()
        sb.setStyleSheet("QStatusBar{background:#1c2128;color:#8b949e;font-size:12px;}"
                         "QStatusBar::item{border:none;}")

        self.lbl_status = QLabel("  Not connected")
        self.lbl_status.setStyleSheet("color:#f85149;font-weight:700;padding:0 6px;")
        sb.addWidget(self.lbl_status)

        sep1 = QLabel("|"); sep1.setStyleSheet("color:#30363d;"); sb.addWidget(sep1)
        self.lbl_blocks = QLabel("Blocks: -")
        self.lbl_blocks.setStyleSheet("color:#8b949e;padding:0 6px;"); sb.addWidget(self.lbl_blocks)
        sep2 = QLabel("|"); sep2.setStyleSheet("color:#30363d;"); sb.addWidget(sep2)
        self.lbl_peers = QLabel("Peers: -")
        self.lbl_peers.setStyleSheet("color:#8b949e;padding:0 6px;"); sb.addWidget(self.lbl_peers)
        sep3 = QLabel("|"); sep3.setStyleSheet("color:#30363d;"); sb.addWidget(sep3)

        self.sync_bar = QProgressBar()
        self.sync_bar.setObjectName("sbar")
        self.sync_bar.setRange(0, 10000); self.sync_bar.setValue(0)
        self.sync_bar.setFormat("Connecting")
        self.sync_bar.setFixedHeight(15)
        sb.addWidget(self.sync_bar, 1)

    def _row_addr(self, tbl, pos):
        row = tbl.rowAt(pos.y())
        if row < 0: return ""
        item = tbl.item(row, 0)
        return item.text() if item else ""

    def _prefill_send(self, addr):
        self.tabs.setCurrentIndex(2)
        for i in range(self.combo_from.count()):
            if self.combo_from.itemData(i) == addr:
                self.combo_from.setCurrentIndex(i); return

    def _toggle_max(self, checked: bool):
        self._max_mode = checked
        if checked:
            self._recalc_max()

    def _on_from_changed(self):
        if self._max_mode:
            self._recalc_max()
        else:
            self._update_summary()

    def _on_fee_changed(self):
        if self._max_mode: self._recalc_max()
        self._update_summary()

    def _on_amt_changed(self):
        if not getattr(self, '_setting_max', False) and self._max_mode:
            self._max_mode = False
            self.btn_max.blockSignals(True)
            self.btn_max.setChecked(False)
            self.btn_max.blockSignals(False)
        self._update_summary()

    def _recalc_max(self):
        addr = self.combo_from.currentData()
        if not addr:
            return
        bal = self._addr_balances.get(addr, 0.0)
        fee = self.spin_fee.value()
        bal_sat = round(bal * 1e8)
        fee_sat = round(fee * 1e8)
        amt_sat = bal_sat - fee_sat
        amt = amt_sat / 1e8 if amt_sat > 0 else 0.0
        self._setting_max = True
        self.spin_amt.setValue(amt)
        self._setting_max = False
        self._update_summary()

    def _update_send_btn(self):
        if not hasattr(self, 'btn_send'):
            return
        to = self.e_to.text().strip()
        amt = self.spin_amt.value()
        to_ok = bool(to) and (
            ((to.startswith("t1") or to.startswith("t3")) and len(to) == 35) or
            (to.startswith("zs1") and len(to) == 78) or
            (to.startswith("zc")  and len(to) >= 40)
        )
        self.btn_send.setEnabled(to_ok and amt > 0)

    def _validate_to_addr(self, text: str):
        t = text.strip()
        if not t:
            self.e_to.setStyleSheet("")
        else:
            valid = (
                ((t.startswith("t1") or t.startswith("t3")) and len(t) == 35) or
                (t.startswith("zs1") and len(t) == 78) or
                (t.startswith("zc")  and len(t) >= 40)
            )
            self.e_to.setStyleSheet(
                "" if valid
                else "QLineEdit{border:1px solid #f85149;background:#1c1010;border-radius:6px;}"
            )
        self._update_send_btn()

    def _update_summary(self):
        amt = self.spin_amt.value(); fee = self.spin_fee.value()
        self.lbl_sum_recv.setText(f"{fmt_btcz(amt)}  BTCZ")
        self.lbl_sum_fee.setText(f"{fmt_btcz(fee)}  BTCZ")
        self.lbl_sum_total.setText(f"{fmt_btcz(amt + fee)}  BTCZ")
        self._update_send_btn()

    def _t_ctx(self, pos):
        addr = self._row_addr(self.tbl_t, pos)
        if not addr: return
        m = QMenu(self)
        ac = m.addAction("Copy Address"); asend = m.addAction("Send from this Address")
        m.addSeparator(); aexp = m.addAction("Export Private Key")
        act = m.exec(self.tbl_t.viewport().mapToGlobal(pos))
        if act == ac:
            QApplication.clipboard().setText(addr)
            self.statusBar().showMessage(f"Copied: {addr}", 3000)
        elif act == asend: self._prefill_send(addr)
        elif act == aexp:  self._export_key(addr, is_z=False)

    def _z_ctx(self, pos):
        addr = self._row_addr(self.tbl_z, pos)
        if not addr: return
        m = QMenu(self)
        ac = m.addAction("Copy Address"); asend = m.addAction("Send from this Address")
        m.addSeparator(); aexp = m.addAction("Export Spending Key")
        act = m.exec(self.tbl_z.viewport().mapToGlobal(pos))
        if act == ac:
            QApplication.clipboard().setText(addr)
            self.statusBar().showMessage(f"Copied: {addr}", 3000)
        elif act == asend: self._prefill_send(addr)
        elif act == aexp:  self._export_key(addr, is_z=True)

    def _export_key(self, addr: str, is_z: bool):
        kind  = "spending key" if is_z else "private key"
        funds = "all shielded funds" if is_z else "all funds here"
        title = "Shielded Spending Key" if is_z else "Private Key (WIF)"
        if QMessageBox.warning(self, "Security Warning",
            f"Export {kind} for:\n{addr}\n\n"
            f"Anyone with this key controls {funds}.\n\nContinue?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) != QMessageBox.StandardButton.Yes: return
        try:
            key = self.rpc.z_ExportKey(addr) if is_z else self.rpc.DumpPrivKey(addr)
            KeyDisplayDialog(self, title, addr, key).exec()
        except RPCError as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _import_key(self):
        self._timer.stop()
        ImportKeyDialog(self, self.rpc).exec()
        self._timer.start(30_000)
        self.refresh()

    def _open_cfg(self):
        dlg = ConfigDialog(self, self.rpc.host, self.rpc.port,
                           self.rpc.user, self.rpc.password, str(CONF_PATH))
        if dlg.exec() == QDialog.DialogCode.Accepted:
            h, p, u, pw = dlg.values(); self.rpc = BitcoinZRPC(h, p, u, pw); self.refresh()

    def _open_diag(self):  DiagDialog(self, self.rpc).exec()
    def _open_about(self): AboutDialog(self, self.rpc).exec()

    def _quit_and_stop(self):
        dlg = _DraggableDialog(self)
        dlg.setMinimumWidth(300)
        v = QVBoxLayout(dlg); v.setContentsMargins(24, 22, 24, 22); v.setSpacing(16)
        lbl = QLabel("Stop BitcoinZ node and exit the wallet?")
        lbl.setStyleSheet("font-size:13px;font-weight:900;color:#e6edf3;")
        lbl.setWordWrap(True); lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        v.addWidget(lbl)
        h = QHBoxLayout(); h.setSpacing(10)
        h.addStretch()
        btn_no  = QPushButton("Cancel"); h.addWidget(btn_no)
        btn_yes = QPushButton("Stop && Exit")
        btn_yes.setObjectName("danger"); h.addWidget(btn_yes)
        h.addStretch(); v.addLayout(h)
        btn_yes.clicked.connect(lambda: (setattr(dlg, '_ok', True), dlg.accept()))
        btn_no.clicked.connect(dlg.reject)
        dlg._ok = False
        if not dlg.exec() or not dlg._ok: return
        self._timer.stop()

        self._shutdown_overlay = QFrame(self.centralWidget())
        self._shutdown_overlay.setObjectName("card")
        self._shutdown_overlay.setStyleSheet(
            "QFrame#card{background:#1c2128;border:1px solid #444c56;border-radius:10px;}"
        )
        self._shutdown_overlay.setMinimumWidth(420)
        sv = QVBoxLayout(self._shutdown_overlay)
        sv.setContentsMargins(32, 24, 32, 24); sv.setSpacing(16)
        self._shutdown_lbl = QLabel("Stopping node")
        self._shutdown_lbl.setStyleSheet(
            "color:#e6edf3;font-size:13px;font-weight:600;background:transparent;")
        self._shutdown_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._shutdown_lbl.setWordWrap(True)
        sv.addWidget(self._shutdown_lbl)
        sbar = QProgressBar(); sbar.setObjectName("sbar")
        sbar.setRange(0, 0); sbar.setFixedHeight(16); sbar.setFormat("Please wait")
        sv.addWidget(sbar)
        self._shutdown_overlay.setLayout(sv)
        self._shutdown_overlay.resize(
            self._shutdown_overlay.minimumSizeHint().width() + 40,
            self._shutdown_overlay.minimumSizeHint().height() + 20,
        )
        cw = self.centralWidget()
        ox = max(0, (cw.width()  - self._shutdown_overlay.width())  // 2)
        oy = max(0, (cw.height() - self._shutdown_overlay.height()) // 2)
        self._shutdown_overlay.move(ox, oy)
        self._shutdown_overlay.raise_(); self._shutdown_overlay.show()
        self.setEnabled(False)
        self._shutdown_w = ShutdownWorker(self.rpc)
        self._shutdown_w.status.connect(self._shutdown_lbl.setText)
        self._shutdown_w.done.connect(self._on_shutdown_done)
        _track(self._shutdown_w).start()

    def _on_shutdown_done(self):
        if hasattr(self, '_shutdown_overlay'):
            self._shutdown_overlay.hide()
            self._shutdown_overlay.deleteLater()
        self.setEnabled(True)
        QApplication.quit()

    def _new_t(self):
        try:
            addr = self.rpc.getNewAddress()
            self.statusBar().showMessage(f"New T-Address: {addr}", 6000); self.refresh()
        except RPCError as e:
            QMessageBox.critical(self, "Error", str(e))

    def _new_z(self):
        try:
            addr = self.rpc.z_getNewAddress()
            self.statusBar().showMessage(f"New Z-Address: {addr}", 6000); self.refresh()
        except RPCError as e:
            QMessageBox.critical(self, "Error", str(e))

    def refresh(self):
        if self._refresh_running: return
        self._refresh_running = True
        w = RefreshWorker(self.rpc)
        self._threads.append(w)
        w.finished.connect(lambda: self._threads.remove(w) if w in self._threads else None)
        w.step.connect(lambda s: None)
        w.done.connect(self._on_done)
        w.error.connect(self._on_err)
        w.reindexing.connect(self._on_reindexing)
        _track(w).start()

    def _on_done(self, data: dict):
        self._refresh_running = False
        self._data = data
        info  = data.get("info",  {}); chain = data.get("chain", {})
        blocks = info.get("blocks", "-"); peers = info.get("connections", "-")
        try: self._cur_blocks = int(blocks)
        except Exception: pass

        self.lbl_status.setText("  Connected")
        self.lbl_status.setStyleSheet("color:#238636;font-weight:700;padding:0 6px;")
        self.lbl_blocks.setText(f"Blocks: {blocks}")
        self.lbl_peers.setText(f"Peers: {peers}")

        vp = chain.get("verificationprogress")
        if vp is not None:
            pct = float(vp) * 100
            self.sync_bar.setValue(int(pct * 100))
            synced = pct >= 99.9
            self.sync_bar.setFormat(f"{'Synced' if synced else 'Syncing'}  {pct:.2f}%")
            self.sync_bar.setStyleSheet(
                f"QProgressBar#sbar::chunk{{background:"
                f"{'#238636' if synced else '#f7a32c'};border-radius:3px}}")
            self._act_import.setEnabled(synced)
        else:
            self.sync_bar.setValue(0); self.sync_bar.setFormat("-")
            self._act_import.setEnabled(False)

        tb        = data.get("total_bal",  {})
        new_t_bal = data.get("t_balances", {})
        new_z_bal = data.get("z_balances", {})

        self._addr_balances.update(new_t_bal)
        self._addr_balances.update(new_z_bal)

        total_val  = float(tb.get("total",       0))
        priv_val   = float(tb.get("private",     0))
        transp_val = float(tb.get("transparent", 0))

        if total_val > 0 or not self._had_balance:
            self.lbl_total.setText(fmt_btcz(total_val))
            self.lbl_priv.setText(fmt_btcz(priv_val))
            self.lbl_transp.setText(fmt_btcz(transp_val))
            if total_val > 0: self._had_balance = True

        self._fill_t_table(new_t_bal)
        self._fill_z_table(new_z_bal)

        prev_data = self.combo_from.currentData()
        self._fill_combo_from(
            data.get("t_addrs", []), data.get("z_addrs", []),
            new_t_bal, new_z_bal, prev_data
        )
        self._update_summary()

        self._cached_txs = data.get("txs", [])
        new_key = str(len(self._cached_txs)) + (
            self._cached_txs[0].get("txid", "") if self._cached_txs else ""
        ) + (self._cached_txs[-1].get("txid", "") if self._cached_txs else "")
        if new_key != self._tx_cache_key:
            self._tx_cache_key = new_key
            self._fill_tx(self._cached_txs)

    def _on_err(self, msg: str):
        self._refresh_running = False
        self._act_import.setEnabled(False)
        self.lbl_status.setText("  Not connected")
        self.lbl_status.setStyleSheet("color:#f85149;font-weight:700;padding:0 6px;")
        self.sync_bar.setValue(0); self.sync_bar.setFormat("Not connected")
        if not self._data:
            box = QMessageBox(self); box.setWindowTitle("Connection Failed")
            box.setIcon(QMessageBox.Icon.Warning)
            box.setText("<b>Cannot connect to BitcoinZ daemon.</b>")
            box.setInformativeText(
                msg.replace("\n", "<br>") + "<br><br>Click <b>Diagnostics</b> for details.")
            bd = box.addButton("Diagnostics", QMessageBox.ButtonRole.ActionRole)
            box.addButton(QMessageBox.StandardButton.Ok); box.exec()
            if box.clickedButton() == bd: self._open_diag()

    def _on_reindexing(self, data: dict):
        self._refresh_running = False
        self._act_import.setEnabled(False)
        info  = data.get("info",  {})
        chain = data.get("chain", {})
        blocks  = info.get("blocks",  "-")
        headers = info.get("headers", "-")
        peers   = info.get("connections", "-")

        self.lbl_status.setText("  Synchronizing...")
        self.lbl_status.setStyleSheet("color:#f7a32c;font-weight:700;padding:0 6px;")
        self.lbl_blocks.setText(f"Blocks: {blocks} / {headers}")
        self.lbl_peers.setText(f"Peers: {peers}")

        vp = chain.get("verificationprogress")
        if vp is not None:
            pct = float(vp) * 100
        elif str(headers).isdigit() and str(blocks).isdigit():
            h = int(headers); b = int(blocks)
            pct = (b / h * 100) if h > 0 else 0.0
        else:
            pct = 0.0

        self.sync_bar.setValue(int(pct * 100))
        self.sync_bar.setFormat(f"Synchronization  {pct:.2f}%")
        self.sync_bar.setStyleSheet(
            "QProgressBar#sbar::chunk{background:#f7a32c;border-radius:3px}")

    def _toggle_sort(self):
        self._tx_desc = not self._tx_desc
        self.btn_sort.setText("↓ Newest first" if self._tx_desc else "↑ Oldest first")
        self._fill_tx(self._cached_txs)

    def _fill_tx(self, txs: list):
        seen_txids: set = set()
        unique: list = []
        for tx in txs:
            tid = tx.get("txid", "")
            if tid and tid not in seen_txids:
                seen_txids.add(tid); unique.append(tx)
            elif not tid:
                unique.append(tx)

        sorted_txs = sorted(unique, key=tx_ts, reverse=self._tx_desc)

        existing_ids: list[str] = []
        for r in range(self.tbl_tx.rowCount()):
            it = self.tbl_tx.item(r, 0)
            tx_d = it.data(Qt.ItemDataRole.UserRole) if it else None
            existing_ids.append(tx_d.get("txid", "") if tx_d else "")

        new_ids = [tx.get("txid", "") for tx in sorted_txs]

        if existing_ids == new_ids:
            for r, tx in enumerate(sorted_txs):
                confirms = int(tx.get("confirmations", 0))
                if confirms < 0:    status_txt, sc = "✗", QColor("#f85149")
                elif confirms == 0: status_txt, sc = "⏳", QColor("#f7a32c")
                else:               status_txt, sc = "✓", QColor("#238636")
                si = self.tbl_tx.item(r, 2)
                if si:
                    si.setText(status_txt); si.setForeground(sc)
                    si.setToolTip(f"{confirms} confirmation{'s' if confirms != 1 else ''}")
            return

        vsb = self.tbl_tx.verticalScrollBar()
        scroll_val = vsb.value() if vsb else 0

        self.tbl_tx.setUpdatesEnabled(False)
        self.tbl_tx.setRowCount(0)

        for tx in sorted_txs:
            r = self.tbl_tx.rowCount(); self.tbl_tx.insertRow(r)
            self.tbl_tx.setRowHeight(r, 26)

            ts = tx.get("blocktime") or tx.get("time") or tx.get("timereceived")
            di = QTableWidgetItem(fmt_ts(ts))
            di.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            di.setForeground(QColor("#8b949e"))
            di.setData(Qt.ItemDataRole.UserRole, tx)
            self.tbl_tx.setItem(r, 0, di)

            addr = tx.get("address", "")
            if not addr:
                short_addr = "🛡 Shielded z-address"
                addr_tip   = "Shielded transaction — address hidden for privacy"
            else:
                short_addr = _fmt_addr(addr)
                addr_tip   = addr
            ai = QTableWidgetItem(short_addr)
            ai.setToolTip(addr_tip); ai.setForeground(QColor("#8b949e"))
            self.tbl_tx.setItem(r, 1, ai)

            confirms = int(tx.get("confirmations", 0))
            if confirms < 0:    status_txt, sc = "✗", QColor("#f85149")
            elif confirms == 0: status_txt, sc = "⏳", QColor("#f7a32c")
            else:               status_txt, sc = "✓", QColor("#238636")
            si = QTableWidgetItem(status_txt)
            si.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            si.setForeground(sc)
            si.setToolTip(f"{confirms} confirmation{'s' if confirms != 1 else ''}")
            self.tbl_tx.setItem(r, 2, si)

            amt = float(tx.get("amount", 0))
            cat = tx.get("category", "")
            sign = "+" if amt >= 0 else ""
            mi = QTableWidgetItem(f"{sign}{fmt_btcz(amt)} BTCZ")
            mi.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            mi.setForeground(QColor("#f85149") if (amt < 0 or cat == "send") else QColor("#238636"))
            self.tbl_tx.setItem(r, 3, mi)

        self.tbl_tx.setUpdatesEnabled(True)

        if vsb:
            vsb.setValue(min(scroll_val, vsb.maximum()))

    def _tx_double_click(self, index):
        item = self.tbl_tx.item(index.row(), 0)
        if item:
            tx = item.data(Qt.ItemDataRole.UserRole)
            if tx: TxDetailDialog(self, tx, self.rpc).exec()

    def _tx_ctx(self, pos):
        row = self.tbl_tx.rowAt(pos.y())
        if row < 0: return
        item = self.tbl_tx.item(row, 0)
        if not item: return
        tx = item.data(Qt.ItemDataRole.UserRole)
        if not tx: return
        m = QMenu(self)
        a_det  = m.addAction("Transaction Details")
        m.addSeparator()
        a_copy = m.addAction("Copy TxID")
        a_addr = m.addAction("Copy Address")
        act = m.exec(self.tbl_tx.viewport().mapToGlobal(pos))
        if act == a_det:
            TxDetailDialog(self, tx, self.rpc).exec()
        elif act == a_copy:
            QApplication.clipboard().setText(tx.get("txid", ""))
            self.statusBar().showMessage("TxID copied", 3000)
        elif act == a_addr:
            QApplication.clipboard().setText(tx.get("address", ""))
            self.statusBar().showMessage("Address copied", 3000)

    def _do_send(self):
        frm  = self.combo_from.currentData()
        to   = self.e_to.text().strip()
        amt  = round(self.spin_amt.value(), 8)
        fee  = round(self.spin_fee.value(), 8)
        memo = self.e_memo.text().strip()
        if not frm:
            QMessageBox.warning(self, "Missing", "Select a From address.")
            return
        bal = round(self._addr_balances.get(frm, 0.0), 8)

        def _show_err(title: str, available: str, required: str, note: str):
            dlg_err = _DraggableDialog(self)
            dlg_err.setMinimumWidth(340)
            ev = QVBoxLayout(dlg_err)
            ev.setContentsMargins(24, 22, 24, 22); ev.setSpacing(10)
            t_err = QLabel(title)
            t_err.setStyleSheet("font-size:14px;font-weight:700;color:#f85149;")
            t_err.setAlignment(Qt.AlignmentFlag.AlignCenter)
            ev.addWidget(t_err)
            div_e = QFrame(); div_e.setFixedHeight(1)
            div_e.setStyleSheet("background:#30363d;"); ev.addWidget(div_e)
            def _erow(lbl_text, val_text, val_color="#e6edf3"):
                h = QHBoxLayout(); h.setSpacing(8)
                l = QLabel(lbl_text)
                l.setStyleSheet("color:#8b949e;font-size:12px;min-width:80px;")
                h.addWidget(l)
                vl = QLabel(val_text)
                vl.setStyleSheet(f"font-size:12px;font-weight:600;color:{val_color};")
                h.addWidget(vl, 1); ev.addLayout(h)
            if available: _erow("Available:", available, "#238636")
            if required:  _erow("Required:",  required,  "#f85149")
            div_e2 = QFrame(); div_e2.setFixedHeight(1)
            div_e2.setStyleSheet("background:#30363d;"); ev.addWidget(div_e2)
            n = QLabel(note)
            n.setStyleSheet("color:#8b949e;font-size:11px;")
            n.setWordWrap(True); n.setAlignment(Qt.AlignmentFlag.AlignCenter)
            ev.addWidget(n)
            btn_ok = QPushButton("OK"); btn_ok.setObjectName("primary")
            btn_ok.setMinimumWidth(90); btn_ok.clicked.connect(dlg_err.accept)
            hb = QHBoxLayout(); hb.addStretch(); hb.addWidget(btn_ok); hb.addStretch()
            ev.addLayout(hb); dlg_err.exec()

        if fee >= bal:
            _show_err("Fee Too Large",
                      f"{fmt_btcz(bal)} BTCZ",
                      f"{fmt_btcz(fee)} BTCZ",
                      "Fee must be less than the available balance.")
            return

        if amt <= 0:
            _show_err("Invalid Amount", "", "",
                      "Amount must be greater than zero.")
            return

        if fee >= amt:
            _show_err("Fee Too Large",
                      f"{fmt_btcz(amt)} BTCZ (amount)",
                      f"{fmt_btcz(fee)} BTCZ (fee)",
                      "Fee must be smaller than the amount being sent.")
            return

        if amt + fee > bal:
            _show_err("Insufficient Funds",
                      f"{fmt_btcz(bal)} BTCZ",
                      f"{fmt_btcz(amt + fee)} BTCZ",
                      "Not enough balance on the selected address.")
            return
        dlg = _DraggableDialog(self)
        dlg.setMinimumWidth(520)
        root = QVBoxLayout(dlg)
        root.setContentsMargins(16, 18, 16, 16)
        root.setSpacing(0)

        title = QLabel("Confirm Transaction")
        title.setStyleSheet("font-size:15px;font-weight:700;color:#e6edf3;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        root.addWidget(title)
        root.addSpacing(14)

        def _add_row(label: str, value: str, value_style: str = "color:#e6edf3;"):
            row = QHBoxLayout(); row.setSpacing(8)
            lbl = QLabel(label)
            lbl.setStyleSheet("color:#8b949e;font-size:12px;min-width:52px;")
            lbl.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            lbl.setSizePolicy(lbl.sizePolicy().horizontalPolicy(),
                              lbl.sizePolicy().verticalPolicy())
            row.addWidget(lbl)
            val = QLabel(value)
            val.setStyleSheet(f"font-size:12px;font-weight:600;{value_style}")
            val.setWordWrap(True)
            val.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
            row.addWidget(val, 1)
            root.addLayout(row)
            root.addSpacing(5)

        _add_row("Amount:",  f"{fmt_btcz(amt)} BTCZ", "color:#e6edf3;")
        _add_row("Fee:",     f"{fmt_btcz(fee)} BTCZ", "color:#8b949e;")

        div = QFrame(); div.setFixedHeight(1)
        div.setStyleSheet("background:#30363d;")
        root.addWidget(div)
        root.addSpacing(5)

        _add_row("Total:",   f"{fmt_btcz(amt + fee)} BTCZ", "color:#f7a32c;font-size:13px;")

        root.addSpacing(5)
        div2 = QFrame(); div2.setFixedHeight(1)
        div2.setStyleSheet("background:#30363d;")
        root.addWidget(div2)
        root.addSpacing(8)

        _add_row("From:", frm, "color:#8b949e;font-family:Consolas,'Courier New',monospace;font-size:12px;")
        _add_row("To:",   to,  "color:#58a6ff;font-family:Consolas,'Courier New',monospace;font-size:12px;")
        if memo:
            _add_row("Memo:", memo, "color:#8b949e;")

        root.addSpacing(14)

        btn_row = QHBoxLayout(); btn_row.setSpacing(10)
        btn_row.addStretch()
        btn_cancel = QPushButton("Cancel")
        btn_cancel.setMinimumWidth(100)
        btn_row.addWidget(btn_cancel)
        btn_confirm = QPushButton("Send")
        btn_confirm.setObjectName("primary")
        btn_confirm.setMinimumWidth(100)
        btn_row.addWidget(btn_confirm)
        btn_row.addStretch()
        root.addLayout(btn_row)

        dlg._confirmed = False
        btn_confirm.clicked.connect(lambda: (setattr(dlg, '_confirmed', True), dlg.accept()))
        btn_cancel.clicked.connect(dlg.reject)

        if not dlg.exec() or not dlg._confirmed:
            return
        self.lbl_op.setText("Sending...")
        w = SendWorker(self.rpc, frm, to, amt, fee, memo)
        self._threads.append(w)
        w.finished.connect(lambda: self._threads.remove(w) if w in self._threads else None)
        w.done.connect(self._send_ok)
        w.error.connect(self._send_err)
        _track(w).start()

    def _send_ok(self, opid: str):
        self.lbl_op.setText(f"Submitted!\nOperation ID: {opid}")
        QTimer.singleShot(5000, self.refresh)
        pw = PollWorker(self.rpc, opid)
        self._threads.append(pw)
        pw.finished.connect(lambda: self._threads.remove(pw) if pw in self._threads else None)
        pw.status_update.connect(self.lbl_op.setText)
        pw.success.connect(self._poll_success)
        pw.failed.connect(self._poll_failed)
        _track(pw).start()

    def _poll_success(self, txid: str):
        self.lbl_op.setText(f"Confirmed!\nTxID: {txid}"); self.refresh()

    def _poll_failed(self, msg: str):
        self.lbl_op.setText(f"FAILED: {msg}")

    def _send_err(self, msg: str):
        self.lbl_op.setText(f"ERROR: {msg}"); QMessageBox.critical(self, "Send Error", msg)


class _WalletStylePlaceholder:
    pass


def main():
    QApplication.setHighDpiScaleFactorRoundingPolicy(
        Qt.HighDpiScaleFactorRoundingPolicy.PassThrough
    )
    app = QApplication(sys.argv)
    _base_font = QFont("Segoe UI", 10)
    app.setFont(_base_font)
    app.setStyleSheet(QSS)
    app.setApplicationName("ZSend Wallet")
    app.setWindowIcon(QIcon(resource_path("ZSend_Wallet.ico")))

    probe = QLocalSocket()
    probe.connectToServer(_SINGLE_INSTANCE_KEY)
    if probe.waitForConnected(500):
        probe.write(b"raise"); probe.flush()
        probe.disconnectFromServer()
        sys.exit(0)
    probe.close()

    server = QLocalServer()
    QLocalServer.removeServer(_SINGLE_INSTANCE_KEY)
    server.listen(_SINGLE_INSTANCE_KEY)

    cfg = load_rpc_cfg()
    rpc = BitcoinZRPC(cfg["host"], cfg["port"], cfg["user"], cfg["password"])

    splash = StartupScreen(rpc)
    main_w = None

    def open_main():
        nonlocal main_w
        main_w = MainWindow(rpc)

        def _on_new_connection():
            conn = server.nextPendingConnection()
            if conn:
                conn.waitForReadyRead(200)
                conn.readAll()
                conn.disconnectFromServer()
            if main_w:
                main_w.raise_window.emit()

        server.newConnection.connect(_on_new_connection)
        main_w.show()
        splash.close()

    splash.ready.connect(open_main)
    splash.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
