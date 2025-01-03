from PyQt6 import QtWidgets

from tools.main_app import MainApp
from tools.ui_signal_emitter import ui_signal_emitter
from ui.shio_trade_monitor import Ui_Dialog
import sys


class MainUI:
    def __init__(self, main_app: MainApp):
        self.app = QtWidgets.QApplication(sys.argv)
        self.Dialog = QtWidgets.QDialog()
        self.ui = Ui_Dialog()
        self.Dialog.closeEvent = self.handle_close_event
        self.ui.setupUi(self.Dialog)

        self.main_app = main_app
        # connect ui signals

        ui_signal_emitter.indicator_signal.connect(self.update_indicator)
        ui_signal_emitter.deal_signal.connect(self.append_deal)
        ui_signal_emitter.strategy_signal.connect(self.append_strategy_signal)
        ui_signal_emitter.set_on()

    def update_indicator(self, msg):
        self.ui.textBrowserIndicaor.setText(msg)

    def append_deal(self, msg):
        self.ui.textBrowserDeals.append(msg)

    def append_strategy_signal(self, msg):
        self.ui.textBrowserSignals.append(msg)

    def handle_close_event(self, event):
        # 在關閉時執行清理邏輯或交互
        reply = QtWidgets.QMessageBox.question(
            self.Dialog,
            "確認關閉",
            "你確定要退出嗎？",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No
        )
        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            print("正在退出程序...")
            self.main_app.stop()
            event.accept()  # 接受關閉事件
        else:
            print("取消退出")
            event.ignore()  # 忽略關閉事件

    def start(self):
        self.main_app.start()
        self.Dialog.show()
        sys.exit(self.app.exec())
