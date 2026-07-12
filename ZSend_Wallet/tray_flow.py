from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QObject, Signal
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QMenu, QSystemTrayIcon

from .locales import tr
from .wallet_cache import WalletCache


TRAY_ON_CLOSE_SETTING_KEY = "minimize_to_tray_on_close"


class TrayController(QObject):
    open_requested = Signal()
    stop_exit_requested = Signal()
    exit_requested = Signal()

    def __init__(
        self,
        cache: WalletCache | None,
        icon_path: Path | str,
        *,
        available: bool | None = None,
        parent: QObject | None = None,
    ):
        super().__init__(parent)
        self.cache = cache
        self.icon_path = str(icon_path)
        self.available = QSystemTrayIcon.isSystemTrayAvailable() if available is None else bool(available)
        self.minimize_on_close = self._load_preference()
        self._icon: QSystemTrayIcon | None = None
        self._menu: QMenu | None = None

    def show(self) -> None:
        if not self.available:
            return
        if self._icon is None:
            self._menu = QMenu()
            self._menu.addAction(tr("dialogs.main_window.tray_open_wallet"), self.open_requested.emit)
            self._menu.addAction(tr("dialogs.main_window.tray_stop_node_exit"), self.stop_exit_requested.emit)
            self._menu.addAction(tr("dialogs.main_window.tray_exit"), self.exit_requested.emit)
            self._icon = QSystemTrayIcon(QIcon(self.icon_path), self)
            self._icon.setToolTip(tr("dialogs.ui.app_title"))
            self._icon.setContextMenu(self._menu)
            self._icon.activated.connect(self._on_activated)
        self._icon.show()

    def hide(self) -> None:
        if self._icon is not None:
            self._icon.hide()

    def set_minimize_on_close(self, checked: bool) -> None:
        self.minimize_on_close = bool(checked)
        if self.cache is not None:
            try:
                self.cache.set_app_setting(TRAY_ON_CLOSE_SETTING_KEY, self.minimize_on_close)
            except Exception:
                pass

    def should_hide_on_close(self) -> bool:
        return bool(
            self.available
            and self.minimize_on_close
            and self._icon is not None
            and self._icon.isVisible()
        )

    def _load_preference(self) -> bool:
        if self.cache is None:
            return True
        try:
            return bool(self.cache.get_app_setting(TRAY_ON_CLOSE_SETTING_KEY, True))
        except Exception:
            return True

    def _on_activated(self, reason) -> None:
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self.open_requested.emit()
