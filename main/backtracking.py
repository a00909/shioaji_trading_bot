from datetime import datetime

from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.order_placer import OrderPlacer
from tick_manager.history_tick_manager import HistoryTickManager
from tools.app import App
from tick_manager.rtm.dummy_rtm import DummyRealtimeTickManager
from tools.dummy_shioaji import DummyShioaji
from tools.plotter import plotter

plotter.active()
start = datetime.now()
app = App(init=True)
contract = app.api.Contracts.Futures.TMF.TMFR1
account = app.api.futopt_account
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
dummy_rtm = DummyRealtimeTickManager(contract, htm, app.redis, '2024-12-31')
dummy_api = DummyShioaji(htm, dummy_rtm, account)
op = OrderPlacer(dummy_api, contract, dummy_api.foutopt_account)
ip = IndicatorProvider(dummy_rtm)
strategy_runner = TMFStrategyRunner(htm, op, ip)
strategy_runner.start()
strategy_runner.wait_for_finish()

profit_loss = dummy_api.list_profit_loss(None)

print(profit_loss)
print(f'total consume: {datetime.now() - start}')
plotter.plot()
app.shut()
