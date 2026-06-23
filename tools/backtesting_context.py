from datetime import timedelta

from data_manager.history.htm2 import NpyCachedHistoryTickManager
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.iiva2 import IntradayIntervalVolumeAvg2
from tools.app.app import App
from data_manager.history.kbm2 import KBarManager2
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.intraday_interval_volume_avg import (
    IntradayIntervalVolumeAvg,
)


class BacktestingContext:
    def __init__(self):
        self.app = App()
        self.contract = self.app.api.Contracts.Futures.TMF.TMFR1  # todo: 不知道怎麼做成讓使用者輸入，暫時寫死
        self.htm = self.app.history_tick_manager
        self.npy_htm = NpyCachedHistoryTickManager(self.app.api)
        self.kbm = KBarManager2(self.app.api) # 暫時沒用到
        self.iiva = IntradayIntervalVolumeAvg2(self.app.api)


    def shut(self):
        self.app.shut()
