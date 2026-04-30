from datetime import timedelta

from tools.app import App
from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.kbar_manager import KBarManager
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.intraday_interval_volume_avg import (
    IntradayIntervalVolumeAvg,
)


class BacktestingContext:
    def __init__(self):
        self.app = App(init=True)
        self.contract = self.app.api.Contracts.Futures.TMF.TMFR1  # todo: 不知道怎麼做成讓使用者輸入，暫時寫死
        self.htm = HistoryTickManager(self.app.api, self.app.redis, self.app.session_maker)
        self.kbm = KBarManager(self.app.api, self.app.redis, self.app.session_maker)
        self.iiva = IntradayIntervalVolumeAvg(self.contract, self.kbm, self.app.redis)

    def iiva_lookup(self, ts):
        return self.iiva.get(ts.replace(second=0, microsecond=0), timedelta(days=30), 5)
