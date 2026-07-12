from __future__ import annotations

import time

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QFrame, QHBoxLayout, QLabel, QProgressBar, QPushButton, QVBoxLayout

from .common import _RUNNING_WORKERS, _track
from .dialogs import _DraggableDialog
from .locales import tr
from .workers import ShutdownWorker


def stop_gui_workers(*, exclude=(), timeout_seconds: float = 0.8) -> None:
    excluded = {worker for worker in exclude if worker is not None}
    workers = [worker for worker in list(_RUNNING_WORKERS) if worker not in excluded]
    for worker in workers:
        if hasattr(worker, "stop"):
            try:
                worker.stop()
            except Exception:
                pass
        else:
            try:
                worker.requestInterruption()
            except Exception:
                pass

    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    for worker in workers:
        try:
            if not worker.isRunning():
                continue
            remaining_ms = int(max(0.0, deadline - time.monotonic()) * 1000)
            if remaining_ms <= 0:
                break
            worker.wait(remaining_ms)
        except Exception:
            pass


def start_shutdown(window) -> None:
    if getattr(window, "_shutdown_w", None) is not None:
        return
    dlg = _DraggableDialog(window)
    dlg.setMinimumWidth(300)
    v = QVBoxLayout(dlg)
    v.setContentsMargins(24, 22, 24, 22)
    v.setSpacing(16)

    lbl = QLabel(tr("dialogs.shutdown_flow.confirm"))
    lbl.setStyleSheet("font-size:13px;font-weight:900;color:#e6edf3;")
    lbl.setWordWrap(True)
    lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    v.addWidget(lbl)

    h = QHBoxLayout()
    h.setSpacing(10)
    h.addStretch()
    btn_no = QPushButton(tr("dialogs.address_actions.cancel"))
    h.addWidget(btn_no)
    btn_yes = QPushButton(tr("dialogs.shutdown_flow.stop_exit"))
    btn_yes.setObjectName("danger")
    h.addWidget(btn_yes)
    h.addStretch()
    v.addLayout(h)

    btn_yes.clicked.connect(lambda: (setattr(dlg, "_ok", True), dlg.accept()))
    btn_no.clicked.connect(dlg.reject)
    dlg._ok = False
    if not dlg.exec() or not dlg._ok:
        return

    refresh_controller = getattr(window, "_refresh_controller", None)
    if refresh_controller is not None:
        refresh_controller.pause_all()
    maintenance = getattr(window, "_maintenance_controller", None)
    if maintenance is not None:
        maintenance.cancel_for_shutdown()
    window._shutdown_overlay = QFrame(window.centralWidget())
    window._shutdown_overlay.setObjectName("card")
    window._shutdown_overlay.setStyleSheet(
        "QFrame#card{background:#1c2128;border:1px solid #444c56;border-radius:10px;}"
    )
    window._shutdown_overlay.setMinimumWidth(420)

    sv = QVBoxLayout(window._shutdown_overlay)
    sv.setContentsMargins(32, 24, 32, 24)
    sv.setSpacing(16)

    window._shutdown_lbl = QLabel(tr("dialogs.shutdown_flow.stopping"))
    window._shutdown_lbl.setStyleSheet(
        "color:#e6edf3;font-size:13px;font-weight:600;background:transparent;"
    )
    window._shutdown_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
    window._shutdown_lbl.setWordWrap(True)
    sv.addWidget(window._shutdown_lbl)

    sbar = QProgressBar()
    sbar.setObjectName("sbar")
    sbar.setRange(0, 0)
    sbar.setFixedHeight(16)
    sbar.setFormat(tr("dialogs.shutdown_flow.please_wait"))
    sv.addWidget(sbar)

    window._shutdown_overlay.setLayout(sv)
    window._shutdown_overlay.resize(
        window._shutdown_overlay.minimumSizeHint().width() + 40,
        window._shutdown_overlay.minimumSizeHint().height() + 20,
    )
    cw = window.centralWidget()
    ox = max(0, (cw.width() - window._shutdown_overlay.width()) // 2)
    oy = max(0, (cw.height() - window._shutdown_overlay.height()) // 2)
    window._shutdown_overlay.move(ox, oy)
    window._shutdown_overlay.raise_()
    window._shutdown_overlay.show()
    window.setEnabled(False)

    window._shutdown_w = ShutdownWorker(window.rpc)
    window._shutdown_w.status.connect(window._shutdown_lbl.setText)
    window._shutdown_w.done.connect(lambda: finish_shutdown(window))
    window._shutdown_w.error.connect(lambda message: fail_shutdown(window, message))
    _track(window._shutdown_w).start()


def finish_shutdown(window) -> None:
    if getattr(window, "_shutdown_overlay", None) is not None:
        window._shutdown_overlay.hide()
        window._shutdown_overlay.deleteLater()
        window._shutdown_overlay = None
    window._shutdown_w = None
    window.setEnabled(True)
    window._exit_gui()


def fail_shutdown(window, message: str) -> None:
    if getattr(window, "_shutdown_overlay", None) is not None:
        window._shutdown_overlay.hide()
        window._shutdown_overlay.deleteLater()
        window._shutdown_overlay = None
    window._shutdown_w = None
    window.setEnabled(True)
    refresh_controller = getattr(window, "_refresh_controller", None)
    if refresh_controller is not None:
        refresh_controller.resume(trigger_refresh=True)
    from .dialogs import _msg_critical

    _msg_critical(window, tr("dialogs.shutdown_flow.error_title"), str(message))
