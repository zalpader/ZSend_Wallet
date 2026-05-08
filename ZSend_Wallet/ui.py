from __future__ import annotations

import time

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtWidgets import QFrame, QLabel, QProgressBar, QTabWidget, QVBoxLayout, QWidget

from .common import _track
from .debug_runtime import debug_log
from .dialogs import _msg_critical, _msg_warning
from .locales import tr
from .rpc import BitcoinZRPC
from .workers import NodeStartWorker, ParamsWorker

QSS = """
QMainWindow,QDialog{background:#1c2128;color:#e6edf3}
QDialog{border:1px solid #444c56;border-radius:8px}
QWidget{background:#1c2128;color:#e6edf3;font-family:'Segoe UI','Segoe UI Emoji',sans-serif;font-size:10pt}
QTabWidget::pane{border:1px solid #30363d;border-radius:6px;background:#1c2128}
QTabBar::tab{background:#1c2128;color:#8b949e;padding:10px 18px;border:none;
  font-weight:600;font-size:13px;min-width:100px}
QTabBar::tab:selected{color:#f7a32c;border-bottom:3px solid #f7a32c;background:#1c2128}
QTabBar::tab:hover{color:#e6edf3}
QTableWidget,QTableView{background:#1c2128;alternate-background-color:#1c2128;color:#e6edf3;
  font-size:13px;gridline-color:#30363d;border:1px solid #30363d;border-radius:6px;
  selection-background-color:#2d333b;outline:0}
QTableWidget::item,QTableView::item{padding:4px 6px;font-size:13px;border:none;outline:0}
QTableWidget::item:selected,QTableView::item:selected{background:#2d333b;color:#f7a32c;font-size:13px;
  padding:4px 6px;border:none;outline:0}
QTableWidget::item:focus,QTableView::item:focus{padding:4px 6px;border:none;outline:0}
QTableWidget::item:selected:focus,QTableView::item:selected:focus{padding:4px 6px;border:none;outline:0}
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
QScrollBar:vertical{background:#1c2128;width:10px;margin:2px}
QScrollBar::handle:vertical{background:#6e7681;border-radius:5px;min-height:28px}
QScrollBar::handle:vertical:hover{background:#f7a32c}
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



def mk_card(title, widget):
    c = QFrame(); c.setObjectName("card")
    v = QVBoxLayout(c); v.setContentsMargins(16, 12, 16, 12); v.setSpacing(4)
    lbl = QLabel(title.upper())
    lbl.setStyleSheet("color:#8b949e;font-size:11px;font-weight:700;font-family:'Segoe UI','Segoe UI Emoji',sans-serif;")
    c._title_label = lbl
    v.addWidget(lbl); v.addWidget(widget); return c


def slbl(text):
    l = QLabel(text)
    l.setStyleSheet("color:#8b949e;font-size:11px;font-weight:700;")
    return l



class StartupScreen(QWidget):
    ready = Signal()
    raise_window = Signal()

    def __init__(self, rpc: BitcoinZRPC):
        super().__init__()
        self.setWindowTitle(tr("dialogs.ui.app_title")); self.resize(520, 260)
        self.setStyleSheet("background:#1c2128;color:#e6edf3;font-family:'Segoe UI';")
        self._rpc = rpc
        self._last_params_debug_ts = 0.0
        v = QVBoxLayout(self); v.setContentsMargins(36, 32, 36, 32); v.setSpacing(14)
        logo = QLabel(tr("dialogs.ui.app_title"))
        logo.setStyleSheet("font-size:22px;font-weight:900;color:#f7a32c;")
        logo.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(logo)
        self.lbl = QLabel(tr("dialogs.ui.startup_checking_params"))
        self.lbl.setStyleSheet("color:#8b949e;font-size:13px;")
        self.lbl.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(self.lbl)
        self.bar = QProgressBar(); self.bar.setObjectName("sbar")
        self.bar.setRange(0, 0); self.bar.setFixedHeight(16); v.addWidget(self.bar)
        self.lbl_size = QLabel("")
        self.lbl_size.setStyleSheet("color:#8b949e;font-size:11px;")
        self.lbl_size.setAlignment(Qt.AlignmentFlag.AlignCenter); v.addWidget(self.lbl_size)

        self._pw = ParamsWorker()
        self._pw.status.connect(self._set_status)
        self._pw.progress.connect(self._on_params_progress)
        self._pw.done.connect(self._params_ok)
        self._pw.failed.connect(self._params_fail)
        _track(self._pw).start()

    def _set_status(self, text: str):
        debug_log("Startup status update", text=text)
        self.lbl.setText(text)

    def _on_params_progress(self, done: int, total: int):
        now = time.monotonic()
        if now - self._last_params_debug_ts >= 5 or done >= total:
            self._last_params_debug_ts = now
            debug_log("Startup params progress", done=done, total=total)
        self.bar.setRange(0, total)
        self.bar.setValue(done)
        mb_done  = done  / 1_048_576
        mb_total = total / 1_048_576
        self.lbl_size.setText(f"{mb_done:.1f} MB / {mb_total:.0f} MB")

    def _params_ok(self):
        self.bar.setRange(0, 0)
        self.lbl_size.setText("")
        debug_log("Params verification finished, starting node worker")
        self.lbl.setText(tr("dialogs.ui.startup_starting_node"))
        self._nw = NodeStartWorker(self._rpc)
        self._nw.status.connect(self._set_status)
        self._nw.ready.connect(self._ok)
        self._nw.failed.connect(self._fail)
        _track(self._nw).start()

    def _params_fail(self, msg: str):
        debug_log("Startup params failure", message=msg)
        self.bar.setRange(0, 1); self.bar.setValue(0)
        self.lbl.setText(tr("dialogs.ui.startup_error", message=msg.splitlines()[0]))
        _msg_critical(None, tr("dialogs.ui.params_error_title"), msg)
        QTimer.singleShot(500, self.ready.emit)

    def _ok(self):
        debug_log("Startup node ready")
        self.bar.setRange(0, 1); self.bar.setValue(1)
        self.lbl.setText(tr("dialogs.ui.startup_ready"))
        self.lbl_size.setText("")
        QTimer.singleShot(500, self.ready.emit)

    def _fail(self, msg):
        debug_log("Startup node warning", message=msg)
        self.bar.setRange(0, 1); self.bar.setValue(0)
        self.lbl.setText(tr("dialogs.ui.startup_warning", message=msg.splitlines()[0]))
        if len(msg.splitlines()) > 1:
            _msg_warning(None, tr("dialogs.ui.node_connection_failed"), msg)
        QTimer.singleShot(2000, self.ready.emit)



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


