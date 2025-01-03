from PyQt6.QtCore import QObject, pyqtSignal


class UISignalEmitter(QObject):
    indicator_signal = pyqtSignal(str)
    deal_signal = pyqtSignal(str)
    strategy_signal = pyqtSignal(str)

    def __init__(self, on=False):
        super().__init__()

        self.on = on

    def set_on(self):
        self.on = True

    def emit_indicator(self, msg, end='\n'):
        self.__emit(self.indicator_signal, msg + end)

    def emit_deal(self, msg, end='\n'):
        self.__emit(self.deal_signal, msg + end)

    def emit_strategy(self, msg, end='\n'):
        self.__emit(self.strategy_signal, msg + end)

    def __emit(self, signal, msg):
        if self.on:
            signal.emit(msg)


ui_signal_emitter = UISignalEmitter()
