from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.tools.order_placer import OrderPlacer
from tick_manager.history_tick_manager import HistoryTickManager
from tools.app import App
from tools.dummy_rtm import DummyRealtimeTickManager
from tools.dummy_shioaji import DummyShioaji

app = App(init=True)
contract = app.api.Contracts.Futures.TMF.TMFR1
account = app.api.futopt_account
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
dummy_rtm = DummyRealtimeTickManager(contract, htm, app.redis, '2024-12-13')
dummy_api = DummyShioaji(htm, dummy_rtm, account)
op = OrderPlacer(dummy_api, contract, dummy_api.foutopt_account)
strategy_runner = TMFStrategyRunner(dummy_rtm, htm, op)
strategy_runner.start()
strategy_runner.wait_for_finish()

profit_loss = dummy_api.list_profit_loss(None)

print(profit_loss)

app.shut()
