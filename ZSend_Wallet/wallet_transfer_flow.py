from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from secrets import token_hex

from PySide6.QtCore import QObject, Signal

from .common import _track, create_sensitive_text_file
from .locales import tr
from .rpc import BitcoinZRPC
from .wallet_export import FullWalletExportWorker, _sanitize_dump_basename, atomic_write_wallet_json
from .wallet_import import FullWalletImportWorker


@dataclass(frozen=True)
class ImportFileError(Exception):
    kind: str
    detail: str = ""

    def __str__(self) -> str:
        return self.detail or self.kind


class WalletTransferController(QObject):
    started = Signal(str)
    export_finished = Signal(object)
    import_finished = Signal()
    failed = Signal(str, str)
    active_changed = Signal(bool)

    def __init__(self, rpc: BitcoinZRPC, parent: QObject | None = None):
        super().__init__(parent)
        self.rpc = rpc
        self._active = False
        self._operation = ""
        self._worker = None
        self._pending_dump_text = ""
        self._temp_dump_path: Path | None = None
        self._generation = 0

    @property
    def active(self) -> bool:
        return self._active

    @property
    def operation(self) -> str:
        return self._operation

    def set_rpc(self, rpc: BitcoinZRPC) -> None:
        self.rpc = rpc

    def prepare_import(self, path: Path | str) -> None:
        if self._active:
            raise ImportFileError("busy")
        self.discard_prepared_import()
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:
            raise ImportFileError("read", str(exc)) from exc
        if not isinstance(payload, dict) or payload.get("format") != "zsend_wallet_export_v1":
            payload = {}
            raise ImportFileError("unsupported")
        dump_text = str(payload.get("node_dump_text") or "").strip()
        payload = {}
        if not dump_text:
            raise ImportFileError("missing_dump")
        self._pending_dump_text = dump_text
        dump_text = ""

    def discard_prepared_import(self) -> None:
        self._pending_dump_text = ""

    def start_import(self) -> bool:
        if self._active or not self._pending_dump_text:
            return False
        self._generation += 1
        generation = self._generation
        try:
            self._temp_dump_path = create_sensitive_text_file(
                "zsend_import_",
                ".dump",
                self._pending_dump_text + "\n",
            )
        except Exception as exc:
            self.discard_prepared_import()
            self.failed.emit("import", str(exc))
            return False
        self.discard_prepared_import()
        worker = FullWalletImportWorker(self.rpc, self._temp_dump_path)
        self._begin("import", worker)
        worker.done.connect(lambda g=generation, w=worker: self._on_import_done(g, w))
        worker.error.connect(lambda message, g=generation, w=worker: self._on_error(g, w, "import", message))
        worker.finished.connect(lambda g=generation, w=worker: self._on_worker_finished(g, w))
        _track(worker).start()
        self.started.emit("import")
        return True

    def start_export(self, export_dir: Path, output_path: Path) -> bool:
        if self._active:
            return False
        self._generation += 1
        generation = self._generation
        dump_basename = _sanitize_dump_basename(f"zsendexport{token_hex(16)}")
        worker = FullWalletExportWorker(self.rpc, Path(export_dir), dump_basename)
        self._begin("export", worker)
        worker.done.connect(
            lambda payload, g=generation, w=worker, path=Path(output_path):
            self._on_export_done(g, w, path, payload)
        )
        worker.error.connect(lambda message, g=generation, w=worker: self._on_error(g, w, "export", message))
        worker.finished.connect(lambda g=generation, w=worker: self._on_worker_finished(g, w))
        _track(worker).start()
        self.started.emit("export")
        return True

    def stop(self) -> None:
        self._generation += 1
        self.discard_prepared_import()
        self._cleanup_temp_dump()
        worker = self._worker
        if worker is not None:
            try:
                worker.requestInterruption()
            except Exception:
                pass
        self._worker = None
        self._set_inactive()

    def _begin(self, operation: str, worker) -> None:
        self._operation = operation
        self._worker = worker
        self._active = True
        self.active_changed.emit(True)

    def _valid(self, generation: int, worker) -> bool:
        return generation == self._generation and worker is self._worker

    def _on_export_done(self, generation: int, worker, output_path: Path, payload: dict) -> None:
        if not self._valid(generation, worker):
            return
        try:
            atomic_write_wallet_json(output_path, payload)
        except Exception as exc:
            self._finish()
            self.failed.emit("export", str(exc))
            return
        summary = dict(payload.get("summary") or {})
        payload.clear()
        self._finish()
        self.export_finished.emit({"path": output_path, "summary": summary})

    def _on_import_done(self, generation: int, worker) -> None:
        if not self._valid(generation, worker):
            return
        self._cleanup_temp_dump()
        self._finish()
        self.import_finished.emit()

    def _on_error(self, generation: int, worker, operation: str, message: str) -> None:
        if not self._valid(generation, worker):
            return
        self._cleanup_temp_dump()
        self._finish()
        self.failed.emit(operation, str(message))

    def _on_worker_finished(self, generation: int, worker) -> None:
        if not self._valid(generation, worker):
            return
        operation = self._operation
        self._cleanup_temp_dump()
        self._finish()
        self.failed.emit(operation, tr("dialogs.main_window.wallet_transfer_incomplete"))

    def _finish(self) -> None:
        self._worker = None
        self._set_inactive()

    def _set_inactive(self) -> None:
        if self._active:
            self._active = False
            self.active_changed.emit(False)
        self._operation = ""

    def _cleanup_temp_dump(self) -> None:
        path = self._temp_dump_path
        self._temp_dump_path = None
        if path is None:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
