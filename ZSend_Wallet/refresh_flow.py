from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from PySide6.QtCore import QObject, QTimer, Signal

from .common import _track, is_port_open
from .rpc import BitcoinZRPC
from .wallet_cache import WalletCache
from .wallet_import import read_recent_wallet_rescan_state
from .workers import RefreshWorker, StatusWorker, TxStatusBatchWorker


@dataclass(frozen=True)
class NodeState:
    mode: str
    blocks: object = "-"
    peers: object = "-"
    progress_value: int = 0
    percent: float | None = None
    payload: dict | None = None
    chain: dict | None = None
    error: str = ""
    show_connection_dialog: bool = False


class RefreshController(QObject):
    wallet_snapshot = Signal(object)
    node_state = Signal(object)
    fast_tx_updates = Signal(object)
    reindexing = Signal(object)
    refreshing_changed = Signal(bool)

    STATUS_INTERVAL_MS = 2_000
    WALLET_INTERVAL_MS = 30_000
    RECONCILE_INTERVAL_MS = 300_000

    def __init__(
        self,
        rpc: BitcoinZRPC,
        cache: WalletCache | None,
        txid_provider: Callable[[], list[str]],
        parent: QObject | None = None,
    ):
        super().__init__(parent)
        self.rpc = rpc
        self.cache = cache
        self._txid_provider = txid_provider
        self._generation = 0
        self._paused = True
        self._refresh_worker: RefreshWorker | None = None
        self._status_worker: StatusWorker | None = None
        self._tx_worker: TxStatusBatchWorker | None = None
        self._status_rpc_failures = 0
        self._has_wallet_data = False
        self._last_status_block: int | None = None
        self._last_wallet_activity: str | None = None
        self._wallet_ready = False
        self._status_baselined = False
        self._refresh_after_current = False

        self.wallet_timer = QTimer(self)
        self.wallet_timer.setInterval(self.WALLET_INTERVAL_MS)
        self.wallet_timer.timeout.connect(self.refresh)
        self.status_timer = QTimer(self)
        self.status_timer.setInterval(self.STATUS_INTERVAL_MS)
        self.status_timer.timeout.connect(self.refresh_status)
        self.reconcile_timer = QTimer(self)
        self.reconcile_timer.setInterval(self.RECONCILE_INTERVAL_MS)
        self.reconcile_timer.timeout.connect(lambda: self.refresh(force_full=True))

    @property
    def refreshing(self) -> bool:
        return self._refresh_worker is not None

    @property
    def active(self) -> bool:
        return not self._paused

    @property
    def generation(self) -> int:
        return self._generation

    def note_wallet_snapshot(self, data: dict | None) -> None:
        if isinstance(data, dict):
            self._has_wallet_data = True

    def clear_wallet_context(self) -> None:
        self._has_wallet_data = False

    def start(self) -> None:
        self.resume(trigger_refresh=False)
        self.refresh()
        self.refresh_status()

    def set_rpc(self, rpc: BitcoinZRPC, *, refresh: bool = True) -> None:
        was_active = not self._paused
        self.pause_all()
        self.rpc = rpc
        if was_active:
            self.resume(trigger_refresh=refresh, force_full=True)

    def pause_wallet_refresh(self) -> None:
        self.wallet_timer.stop()

    def resume_wallet_refresh(self) -> None:
        if not self._paused and not self.wallet_timer.isActive():
            self.wallet_timer.start()

    def pause_all(self) -> None:
        self._paused = True
        self._generation += 1
        self._status_rpc_failures = 0
        self._last_status_block = None
        self._last_wallet_activity = None
        self._wallet_ready = False
        self._status_baselined = False
        self._refresh_after_current = False
        self.wallet_timer.stop()
        self.status_timer.stop()
        self.reconcile_timer.stop()
        workers = self._detach_workers()
        self._request_worker_stop(workers)

    def resume(self, *, trigger_refresh: bool = False, force_full: bool = False) -> None:
        self._paused = False
        if not self.wallet_timer.isActive():
            self.wallet_timer.start()
        if not self.status_timer.isActive():
            self.status_timer.start()
        if not self.reconcile_timer.isActive():
            self.reconcile_timer.start()
        if trigger_refresh:
            self.refresh(force_full=force_full)
            self.refresh_status()

    def refresh(self, force_full: bool = False) -> None:
        if self._paused or self._refresh_worker is not None:
            return
        generation = self._generation
        worker = RefreshWorker(self.rpc, self.cache, force_full=force_full)
        self._refresh_worker = worker
        self.refreshing_changed.emit(True)
        worker.done.connect(lambda data, g=generation, w=worker: self._on_refresh_done(g, w, data))
        worker.error.connect(lambda message, g=generation, w=worker: self._on_refresh_error(g, w, message))
        worker.reindexing.connect(lambda data, g=generation, w=worker: self._on_reindexing(g, w, data))
        worker.finished.connect(lambda g=generation, w=worker: self._on_refresh_finished(g, w))
        _track(worker).start()

    def refresh_status(self) -> None:
        if self._paused or self._status_worker is not None:
            return
        generation = self._generation
        worker = StatusWorker(self.rpc)
        self._status_worker = worker
        worker.done.connect(lambda data, g=generation, w=worker: self._on_status_done(g, w, data))
        worker.error.connect(lambda message, g=generation, w=worker: self._on_status_error(g, w, message))
        worker.finished.connect(lambda g=generation, w=worker: self._on_status_finished(g, w))
        _track(worker).start()
        self._refresh_fast_transactions(generation)

    def stop(self, wait_ms: int = 800) -> None:
        workers = [self._refresh_worker, self._status_worker, self._tx_worker]
        self.pause_all()
        per_worker = max(0, int(wait_ms / max(1, len([w for w in workers if w is not None]))))
        for worker in workers:
            if worker is None:
                continue
            try:
                worker.wait(per_worker)
                if worker.isRunning():
                    worker.terminate()
                    worker.wait(200)
            except Exception:
                pass

    def _detach_workers(self) -> tuple[object | None, object | None, object | None]:
        workers = (self._refresh_worker, self._status_worker, self._tx_worker)
        had_refresh = self._refresh_worker is not None
        self._refresh_worker = None
        self._status_worker = None
        self._tx_worker = None
        if had_refresh:
            self.refreshing_changed.emit(False)
        return workers

    def _request_worker_stop(self, workers=None) -> None:
        if workers is None:
            workers = (self._refresh_worker, self._status_worker, self._tx_worker)
        for worker in workers:
            if worker is None:
                continue
            try:
                worker.stop()
            except Exception:
                try:
                    worker.requestInterruption()
                except Exception:
                    pass

    def _valid(self, generation: int, worker, current) -> bool:
        return generation == self._generation and worker is current and not self._paused

    def _on_refresh_done(self, generation: int, worker, data: dict) -> None:
        if not self._valid(generation, worker, self._refresh_worker):
            return
        self._status_rpc_failures = 0
        self.note_wallet_snapshot(data)
        self.wallet_snapshot.emit(data)

    def _on_refresh_error(self, generation: int, worker, message: str) -> None:
        if not self._valid(generation, worker, self._refresh_worker):
            return
        self.node_state.emit(self._state_from_refresh_error(message))

    def _on_reindexing(self, generation: int, worker, data: dict) -> None:
        if not self._valid(generation, worker, self._refresh_worker):
            return
        self.reindexing.emit(self._state_from_reindexing(data))

    def _on_refresh_finished(self, generation: int, worker) -> None:
        if worker is self._refresh_worker:
            self._refresh_worker = None
            if generation == self._generation:
                self.refreshing_changed.emit(False)
                if self._refresh_after_current and not self._paused:
                    self._refresh_after_current = False
                    QTimer.singleShot(0, self.refresh)

    def _refresh_fast_transactions(self, generation: int) -> None:
        if self._tx_worker is not None:
            return
        txids = list(self._txid_provider() or [])
        if not txids:
            return
        worker = TxStatusBatchWorker(self.rpc, txids)
        self._tx_worker = worker
        worker.done.connect(lambda updates, g=generation, w=worker: self._on_tx_done(g, w, updates))
        worker.finished.connect(lambda g=generation, w=worker: self._on_tx_finished(g, w))
        _track(worker).start()

    def _on_tx_done(self, generation: int, worker, updates) -> None:
        if self._valid(generation, worker, self._tx_worker):
            self.fast_tx_updates.emit(list(updates or []))

    def _on_tx_finished(self, _generation: int, worker) -> None:
        if worker is self._tx_worker:
            self._tx_worker = None

    def _on_status_done(self, generation: int, worker, data: dict) -> None:
        if not self._valid(generation, worker, self._status_worker):
            return
        self._status_rpc_failures = 0
        self._refresh_for_wallet_activity(data)
        self.node_state.emit(self._state_from_status(data))

    def _refresh_for_wallet_activity(self, data: dict) -> None:
        data = data if isinstance(data, dict) else {}
        chain = data.get("chain") if isinstance(data.get("chain"), dict) else {}
        try:
            block = int(chain.get("blocks"))
        except Exception:
            block = None
        activity = data.get("wallet_activity")
        activity = str(activity) if activity is not None else None
        percent = self._progress_percent(chain)
        ready = bool(
            block is not None
            and not chain.get("reindex")
            and not chain.get("initialblockdownload")
            and percent is not None
            and percent >= 99.9
        )

        became_ready = self._status_baselined and ready and not self._wallet_ready
        block_changed = (
            ready
            and self._last_status_block is not None
            and block != self._last_status_block
        )
        activity_changed = (
            ready
            and activity is not None
            and self._last_wallet_activity is not None
            and activity != self._last_wallet_activity
        )
        self._wallet_ready = ready
        self._status_baselined = True
        if block is not None:
            self._last_status_block = block
        if activity is not None:
            self._last_wallet_activity = activity

        if became_ready or block_changed or activity_changed:
            self._request_wallet_refresh()

    def _request_wallet_refresh(self) -> None:
        if self._paused:
            return
        if self._refresh_worker is not None:
            self._refresh_after_current = True
            return
        self.refresh()

    def _on_status_error(self, generation: int, worker, message: str) -> None:
        if not self._valid(generation, worker, self._status_worker):
            return
        self._status_rpc_failures += 1
        if self._status_rpc_failures < 3:
            return
        if is_port_open(self.rpc.host, self.rpc.port, timeout=0.5):
            self.node_state.emit(NodeState(mode="busy", error=message))
        else:
            self.node_state.emit(NodeState(mode="offline", error=message))

    def _on_status_finished(self, _generation: int, worker) -> None:
        if worker is self._status_worker:
            self._status_worker = None

    @staticmethod
    def _progress_percent(chain: dict) -> float | None:
        value = chain.get("verificationprogress")
        if value is None:
            return None
        try:
            return max(0.0, min(100.0, float(value) * 100.0))
        except Exception:
            return 0.0

    def _state_from_status(self, data: dict) -> NodeState:
        data = data if isinstance(data, dict) else {}
        chain = data.get("chain") if isinstance(data.get("chain"), dict) else {}
        blocks = chain.get("blocks", "-")
        peers = data.get("peers", "-")
        bootstrap = data.get("bootstrap_progress")
        if isinstance(bootstrap, dict):
            return NodeState("bootstrap", blocks, peers, int(bootstrap.get("bar_value", 0) or 0), payload=bootstrap, chain=chain)
        reindex = data.get("reindex_progress")
        if bool(chain.get("reindex")) or isinstance(reindex, dict):
            payload = reindex if isinstance(reindex, dict) else {"phase": "reindex_files", "bar_value": 0}
            return NodeState("reindex", blocks, peers, int(payload.get("bar_value", 0) or 0), payload=payload, chain=chain)
        percent = self._progress_percent(chain)
        if percent is None:
            return NodeState("busy", blocks, peers, chain=chain)
        syncing = bool(chain.get("initialblockdownload") or chain.get("reindex")) or percent < 99.9
        return NodeState("syncing" if syncing else "connected", blocks, peers, int(percent * 100), percent, chain=chain)

    def _state_from_refresh_error(self, message: str) -> NodeState:
        rescan = read_recent_wallet_rescan_state()
        if rescan is not None:
            return NodeState("rescan", payload=rescan, error=message)
        if is_port_open(self.rpc.host, self.rpc.port, timeout=0.5):
            return NodeState("busy", error=message)
        return NodeState(
            "offline",
            error=message,
            show_connection_dialog=not self._has_wallet_data,
        )

    def _state_from_reindexing(self, data: dict) -> NodeState:
        data = data if isinstance(data, dict) else {}
        info = data.get("info") if isinstance(data.get("info"), dict) else {}
        chain = data.get("chain") if isinstance(data.get("chain"), dict) else {}
        blocks = info.get("blocks", "-")
        peers = info.get("connections", "-")
        bootstrap = data.get("bootstrap_progress")
        if isinstance(bootstrap, dict):
            return NodeState("bootstrap", blocks, peers, int(bootstrap.get("bar_value", 0) or 0), payload=bootstrap, chain=chain)
        reindex = data.get("reindex_progress")
        if isinstance(reindex, dict):
            return NodeState("reindex", blocks, peers, int(reindex.get("bar_value", 0) or 0), payload=reindex, chain=chain)
        percent = self._progress_percent(chain)
        if percent is None:
            try:
                headers = int(info.get("headers", 0) or 0)
                current = int(info.get("blocks", 0) or 0)
                percent = (current / headers * 100.0) if headers > 0 else 0.0
            except Exception:
                percent = 0.0
        return NodeState("syncing", blocks, peers, int(percent * 100), percent, chain=chain)
