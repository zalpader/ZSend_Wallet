from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QApplication, QFrame, QHBoxLayout, QLabel, QProgressBar, QPushButton, QVBoxLayout

from .common import _track
from .dialogs import _DraggableDialog
from .locales import tr
from .workers import ShutdownWorker


def start_shutdown(window) -> None:
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

    window._timer.stop()
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
    _track(window._shutdown_w).start()


def finish_shutdown(window) -> None:
    if hasattr(window, "_shutdown_overlay"):
        window._shutdown_overlay.hide()
        window._shutdown_overlay.deleteLater()
    window.setEnabled(True)
    QApplication.quit()
