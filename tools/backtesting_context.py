from datetime import timedelta

from qclaw.backtesting.npy_cached_history_tick_manager import NpyCachedHistoryTickManager
from tools.app.app import App
from data_manager.history_data_manager.kbar_manager import KBarManager
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.intraday_interval_volume_avg import (
    IntradayIntervalVolumeAvg,
)


class BacktestingContext:
    def __init__(self):
        self.app = App()
        self.contract = self.app.api.Contracts.Futures.TMF.TMFR1  # todo: 不知道怎麼做成讓使用者輸入，暫時寫死
        self.htm = self.app.history_tick_manager
        self.npy_htm = NpyCachedHistoryTickManager(self.htm)
        self.kbm = KBarManager(self.app.api, self.app.redis, self.app.session_maker)
        self.iiva = IntradayIntervalVolumeAvg(self.contract, self.kbm, self.app.redis)

    def iiva_lookup(self, ts):
        return self.iiva.get(ts.replace(second=0, microsecond=0), timedelta(days=30), 5)

    def shut(self):
        self.app.shut()
