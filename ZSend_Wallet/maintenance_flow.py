from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from .common import _track
from .rpc import BitcoinZRPC
from .workers import MaintenanceRestartWorker


class MaintenanceController(QObject):
    started = Signal(str)
    status = Signal(str)
    progress = Signal(object)
    finished = Signal(str)
    failed = Signal(str)
    active_changed = Signal(bool)

    def __init__(self, rpc: BitcoinZRPC, parent: QObject | None = None):
        super().__init__(parent)
        self.rpc = rpc
        self._worker: MaintenanceRestartWorker | None = None
        self._mode = ""
        self._active = False
        self._generation = 0

    @property
    def active(self) -> bool:
        return self._active

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def worker(self) -> MaintenanceRestartWorker | None:
        return self._worker

    def set_rpc(self, rpc: BitcoinZRPC) -> None:
        self.rpc = rpc

    def start(self, mode: str) -> bool:
        mode = str(mode or "").strip().lower()
        if mode not in {"rescan", "reindex"} or self._active:
            return False
        self._generation += 1
        generation = self._generation
        worker = MaintenanceRestartWorker(self.rpc, mode)
        self._worker = worker
        self._mode = mode
        self._active = True
        self.active_changed.emit(True)
        self.started.emit(mode)
        worker.status.connect(lambda message, g=generation, w=worker: self._on_status(g, w, message))
        worker.progress.connect(lambda payload, g=generation, w=worker: self._on_progress(g, w, payload))
        worker.done.connect(lambda result, g=generation, w=worker: self._on_done(g, w, result))
        worker.error.connect(lambda message, g=generation, w=worker: self._on_error(g, w, message))
        worker.finished.connect(lambda g=generation, w=worker: self._on_worker_finished(g, w))
        _track(worker).start()
        return True

    def cancel_for_shutdown(self) -> None:
        worker = self._worker
        if worker is not None:
            worker.stop()

    def detach_for_exit(self) -> None:
        worker = self._worker
        if worker is not None:
            worker.detach()

    def stop(self, wait_ms: int = 800) -> None:
        self._generation += 1
        worker = self._worker
        if worker is None:
            self._set_inactive()
            return
        worker.stop()
        try:
            worker.wait(max(0, int(wait_ms)))
        except Exception:
            pass
        self._worker = None
        self._set_inactive()

    def _valid(self, generation: int, worker) -> bool:
        return generation == self._generation and worker is self._worker

    def _on_status(self, generation: int, worker, message: str) -> None:
        if self._valid(generation, worker):
            self.status.emit(str(message))

    def _on_progress(self, generation: int, worker, payload) -> None:
        if self._valid(generation, worker):
            self.progress.emit(payload)

    def _on_done(self, generation: int, worker, mode: str) -> None:
        if not self._valid(generation, worker):
            return
        self._worker = None
        self._generation += 1
        self._set_inactive()
        self.finished.emit(str(mode or self._mode))

    def _on_error(self, generation: int, worker, message: str) -> None:
        if not self._valid(generation, worker):
            return
        self._worker = None
        self._generation += 1
        self._set_inactive()
        self.failed.emit(str(message))

    def _on_worker_finished(self, generation: int, worker) -> None:
        if generation == self._generation and worker is self._worker:
            self._worker = None

    def _set_inactive(self) -> None:
        if self._active:
            self._active = False
            self.active_changed.emit(False)
