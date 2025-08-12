from strategy.runner.abs_strategy_runner import AbsStrategyRunner
from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.strategies.abs_strategy import AbsStrategy
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.order_placer import OrderPlacer
from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager
from tools.app import App


class MainApp:
    def __init__(self, contract=None,stra:type[AbsStrategyRunner]=None):
        self.app = App(init=True)
        print(self.app.api.futopt_account)
        if not contract:
            contract = self.app.api.Contracts.Futures.TXF.TXFR1

        # contract = self.app.api.Contracts.Futures.TMF.TMFR1
        self.rtm = RealtimeTickManager(self.app.api, self.app.redis, contract)
        self.htm = HistoryTickManager(self.app.api, self.app.redis, self.app.session_maker)
        self.op = OrderPlacer(self.app.api, contract, self.app.api.futopt_account)
        self.ip = IndicatorProvider(self.rtm)
        if not stra:
            self.stra = TMFStrategyRunner( self.htm, self.op,self.ip)
        else:
            self.stra = stra( self.htm, self.op,self.ip)
            
    def start(self):
        self.stra.start()

    def stop(self):
        self.stra.stop()
        self.app.shut()
        print('bye bye.')
