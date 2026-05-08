from __future__ import annotations

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QRect, Qt
from PySide6.QtGui import QColor, QPainter
from PySide6.QtWidgets import QApplication, QAbstractItemView, QComboBox, QHeaderView, QStyle, QStyledItemDelegate, QStyleOptionComboBox, QTableView

from .common import group_tx_rows, tx_ts
from .helpers import _fmt_addr, _sort_addr_items, fmt_btcz, fmt_ts, tx_status_code
from .locales import tr


def _style_header(hdr):
    hdr.setHighlightSections(False)
    hdr.setStyleSheet(
        "QHeaderView::section{"
        "background:#21262d;color:#8b949e;padding:8px 12px;"
        "border:none;font-size:12px;font-weight:700;}"
        "QHeaderView::section:hover{"
        "background:#2d333b;color:#e6edf3;font-size:12px;font-weight:700;}"
    )


def mk_view():
    t = QTableView()
    t.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    t.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    t.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    t.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
    t.viewport().setFocusPolicy(Qt.FocusPolicy.ClickFocus)
    t.setShowGrid(False)
    t.setAlternatingRowColors(False)
    t.verticalHeader().setVisible(False)
    t.setWordWrap(False)
    t.setSortingEnabled(False)
    _style_header(t.horizontalHeader())
    return t


class AddressTableModel(QAbstractTableModel):
    def __init__(self, base_label: str):
        super().__init__()
        self.base_label = base_label
        self.sort_mode = "balance_desc"
        self.rows: list[tuple[str, float]] = []
        self.busy_addresses: set[str] = set()

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else 2

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation != Qt.Orientation.Horizontal or role != Qt.ItemDataRole.DisplayRole:
            return None
        if self.sort_mode == "name_asc":
            headers = [tr("dialogs.models.sort_name_asc", label=self.base_label), tr("dialogs.models.balance_header")]
        elif self.sort_mode == "name_desc":
            headers = [tr("dialogs.models.sort_name_desc", label=self.base_label), tr("dialogs.models.balance_header")]
        elif self.sort_mode == "balance_asc":
            headers = [self.base_label, tr("dialogs.models.sort_balance_asc")]
        else:
            headers = [self.base_label, tr("dialogs.models.sort_balance_desc")]
        return headers[section]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        addr, bal = self.rows[index.row()]
        col = index.column()
        is_busy = addr in self.busy_addresses
        if role == Qt.ItemDataRole.DisplayRole:
            if col == 0:
                return f"\U0001f504 {addr}" if is_busy else addr
            return fmt_btcz(float(bal))
        if role == Qt.ItemDataRole.TextAlignmentRole and col == 1:
            return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        if role == Qt.ItemDataRole.ForegroundRole:
            return QColor("#e6edf3") if col == 0 else QColor("#8b949e")
        if role == Qt.ItemDataRole.ToolTipRole:
            if is_busy:
                return tr("dialogs.models.address_busy_tooltip", address=addr)
            return addr
        if role == Qt.ItemDataRole.UserRole:
            return {"address": addr, "balance": float(bal), "busy": is_busy}
        return None

    def set_balances(self, bal_dict: dict[str, float], sort_mode: str, busy_addresses: set[str] | None = None):
        new_busy = set(busy_addresses or set())
        new_rows = _sort_addr_items(list((bal_dict or {}).items()), sort_mode)
        header_changed = self.sort_mode != sort_mode
        data_changed = self.rows != new_rows or self.busy_addresses != new_busy
        if not header_changed and not data_changed:
            return
        if len(new_rows) != len(self.rows):
            self.beginResetModel()
            self.sort_mode = sort_mode
            self.busy_addresses = new_busy
            self.rows = new_rows
            self.endResetModel()
        else:
            self.layoutAboutToBeChanged.emit()
            self.sort_mode = sort_mode
            self.busy_addresses = new_busy
            self.rows = new_rows
            self.layoutChanged.emit()
            if self.rows:
                self.dataChanged.emit(self.index(0, 0), self.index(len(self.rows) - 1, 1))
        if header_changed:
            self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 1)

    def address_at(self, row: int) -> str:
        if 0 <= row < len(self.rows):
            return self.rows[row][0]
        return ""


class TransactionTableModel(QAbstractTableModel):
    def __init__(self):
        super().__init__()
        self.rows: list[dict] = []
        self.sort_key = "date"
        self.desc = True

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else 4

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation != Qt.Orientation.Horizontal or role != Qt.ItemDataRole.DisplayRole:
            return None
        labels = [tr("dialogs.models.date"), tr("dialogs.models.address"), tr("dialogs.models.status"), tr("dialogs.models.amount")]
        active = {"date": 0, "status": 2, "amount": 3}.get(self.sort_key, -1)
        if section == active:
            arrow = "\u2193" if self.desc else "\u2191"
            return f"{arrow} {labels[section]}"
        return labels[section]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        tx = self.rows[index.row()]
        col = index.column()
        confirms = int(tx.get("confirmations", 0) or 0)
        amount = float(tx.get("amount", 0) or 0)
        category = tx.get("category", "")

        status = tx_status_code(tx)
        if status == "stale":
            status_txt, status_color = "?", QColor("#f7a32c")
            status_tip = tr("dialogs.models.status_stale")
        elif status == "reorged":
            status_txt, status_color = "!", QColor("#f7a32c")
            status_tip = tr("dialogs.models.status_reorged")
        elif status in {"failed", "expired"}:
            status_txt, status_color = "X", QColor("#f85149")
            status_tip = tr("dialogs.models.status_expired" if status == "expired" else "dialogs.models.status_failed")
        elif status == "conflicted":
            status_txt, status_color = "X", QColor("#f85149")
            status_tip = tr("dialogs.models.status_conflicted")
        elif status == "pending":
            status_txt, status_color = "...", QColor("#f7a32c")
            status_tip = tr("dialogs.models.status_pending")
        else:
            status_txt, status_color = "OK", QColor("#238636")
            status_tip = tr("dialogs.models.status_confirmed")

        addr = str(tx.get("address") or "")
        if not addr:
            addr_text = tr("dialogs.models.shielded_address")
            addr_tip = tr("dialogs.models.shielded_hidden")
        elif len(tx.get("_entries", [])) > 1:
            addr_text = f"{_fmt_addr(addr)}  {tr('dialogs.models.entry_suffix', count=len(tx.get('_entries', [])) - 1)}"
            addr_tip = addr
        else:
            addr_text = _fmt_addr(addr)
            addr_tip = addr

        if role == Qt.ItemDataRole.DisplayRole:
            if col == 0:
                return fmt_ts(tx.get("blocktime") or tx.get("time") or tx.get("timereceived") or tx.get("created_at"))
            if col == 1:
                return addr_text
            if col == 2:
                return status_txt
            if col == 3:
                sign = "+" if amount >= 0 else ""
                return f"{sign}{fmt_btcz(amount)} BTCZ"
        if role == Qt.ItemDataRole.TextAlignmentRole:
            if col == 0 or col == 2:
                return int(Qt.AlignmentFlag.AlignCenter)
            if col == 3:
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        if role == Qt.ItemDataRole.ForegroundRole:
            if col == 0 or col == 1:
                return QColor("#8b949e")
            if col == 2:
                return status_color
            if col == 3:
                return QColor("#f85149") if (amount < 0 or category == "send") else QColor("#238636")
        if role == Qt.ItemDataRole.ToolTipRole:
            if col == 1:
                return addr_tip
            if col == 2:
                return f"{status_tip} - {confirms} confirmation{'s' if confirms != 1 else ''}"
        if role == Qt.ItemDataRole.UserRole:
            return tx
        return None

    def _status_rank(self, tx: dict) -> tuple[int, int]:
        status = tx_status_code(tx)
        confirms = int(tx.get("confirmations", 0) or 0)
        if status in {"conflicted", "failed", "expired", "reorged"}:
            return (0, confirms)
        if status == "stale":
            return (1, confirms)
        if status == "pending":
            return (1, 0)
        return (2, confirms)

    def _sort_rows_list(self, rows: list[dict]):
        if self.sort_key == "amount":
            key = lambda tx: (float(tx.get("amount", 0) or 0), tx_ts(tx), tx.get("txid", ""))
        elif self.sort_key == "status":
            key = lambda tx: (self._status_rank(tx), tx_ts(tx), tx.get("txid", ""))
        else:
            key = lambda tx: (tx_ts(tx), tx.get("txid", ""))
        rows.sort(key=key, reverse=self.desc)

    def _sort_rows(self):
        self._sort_rows_list(self.rows)

    def set_transactions(self, txs: list[dict]):
        new_rows = group_tx_rows(txs or [])
        self._sort_rows_list(new_rows)
        if new_rows == self.rows:
            return
        if len(new_rows) != len(self.rows):
            self.beginResetModel()
            self.rows = new_rows
            self.endResetModel()
        else:
            self.layoutAboutToBeChanged.emit()
            self.rows = new_rows
            self.layoutChanged.emit()
            if self.rows:
                self.dataChanged.emit(self.index(0, 0), self.index(len(self.rows) - 1, 3))

    def set_sort(self, sort_key: str):
        if sort_key == self.sort_key:
            self.desc = not self.desc
        else:
            self.sort_key = sort_key
            self.desc = True
        self.layoutAboutToBeChanged.emit()
        self._sort_rows()
        self.layoutChanged.emit()
        self.headerDataChanged.emit(Qt.Orientation.Horizontal, 0, 3)

    def transaction_at(self, row: int) -> dict | None:
        if 0 <= row < len(self.rows):
            return self.rows[row]
        return None


class _AddrBalanceDelegate(QStyledItemDelegate):
    _PAD = 10

    def paint(self, painter, option, index):
        self.initStyleOption(option, index)
        style = option.widget.style() if option.widget else QApplication.style()
        style.drawPrimitive(QStyle.PrimitiveElement.PE_PanelItemViewItem, option, painter, option.widget)

        text = index.data(Qt.ItemDataRole.DisplayRole) or ""
        parts = text.rsplit("   ", 1)
        addr = parts[0].strip()
        bal = parts[1].strip() if len(parts) == 2 else ""

        rect: QRect = option.rect.adjusted(self._PAD, 0, -self._PAD, 0)
        color = option.palette.highlightedText().color() if option.state & QStyle.StateFlag.State_Selected else option.palette.text().color()

        painter.save()
        painter.setPen(color)

        bal_w = painter.fontMetrics().horizontalAdvance(bal) if bal else 0
        bal_rect = QRect(rect.right() - bal_w, rect.top(), bal_w, rect.height())
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
        bal = parts[1].strip() if len(parts) == 2 else ""

        rect = style.subControlRect(
            QStyle.ComplexControl.CC_ComboBox, opt, QStyle.SubControl.SC_ComboBoxEditField, self
        ).adjusted(self._PAD, 0, -self._PAD, 0)

        fm = p.fontMetrics()
        p.setPen(self.palette().text().color())

        if bal:
            bal_w = fm.horizontalAdvance(bal)
            addr_rect = QRect(rect.left(), rect.top(), rect.width() - bal_w - self._PAD, rect.height())
            bal_rect = QRect(rect.right() - bal_w, rect.top(), bal_w, rect.height())

            addr_clipped = fm.elidedText(addr, Qt.TextElideMode.ElideMiddle, addr_rect.width())
            p.drawText(addr_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, addr_clipped)

            color = self.palette().text().color()
            color.setAlphaF(0.65)
            p.setPen(color)
            p.drawText(bal_rect, Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter, bal)
        else:
            p.drawText(rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter, fm.elidedText(text, Qt.TextElideMode.ElideMiddle, rect.width()))
        p.end()
