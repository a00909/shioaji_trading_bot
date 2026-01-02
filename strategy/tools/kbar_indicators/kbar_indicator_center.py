from strategy.tools.kbar_indicators.intraday_interval_volume_avg.intraday_interval_volume_avg import \
    IntradayIntervalVolumeAvg
from tick_manager.kbar_manager import KBarManager


class KbarIndicatorCenter:
    def __init__(self, contract, api, redis, session_maker):
        self.contract = contract
        kbm = KBarManager(api, redis, session_maker)
        self.iiva: IntradayIntervalVolumeAvg = IntradayIntervalVolumeAvg(contract, kbm, redis)
