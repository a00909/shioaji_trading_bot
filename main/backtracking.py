from datetime import datetime

from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.order_placer import OrderPlacer
from tick_manager.history_tick_manager import HistoryTickManager
from tools.app import App
from tools.backtracking.dummy_account import DummyAccount
from tools.backtracking.dummy_contract import DummyContract
from tools.backtracking.dummy_rtm import DummyRealtimeTickManager
from tools.backtracking.dummy_shioaji import DummyShioaji
from tools.plotter import plotter
import pandas as pd

plotter.active()
start = datetime.now()
app = App(init=True)
contract = app.api.Contracts.Futures.TMF.TMFR1
# contract = DummyContract('TMFR1')
account = app.api.futopt_account
# account = DummyAccount()
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
dummy_rtm = DummyRealtimeTickManager(contract, htm, app.redis, '2025-02-04')
dummy_api = DummyShioaji(htm, dummy_rtm, account)
op = OrderPlacer(dummy_api, contract, dummy_api.foutopt_account)
ip = IndicatorProvider(dummy_rtm)
strategy_runner = TMFStrategyRunner(htm, op, ip)
strategy_runner.start()
strategy_runner.wait_for_finish()

profit_loss = dummy_api.list_profit_loss(None)

if profit_loss:
    df = pd.DataFrame([item.dict() for item in profit_loss])
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 180)
    print(df[['id', 'pnl', 'entry_price', 'cover_price', 'quantity', 'code', 'date']])
    pnl_sum = df["pnl"].sum()
    tax_sum = df["tax"].sum()
    fee_sum = df["fee"].sum()
    print("\nTotal PnL:", pnl_sum - tax_sum - fee_sum)
    print(f'total consume: {datetime.now() - start} (fee: {fee_sum}, tax: {tax_sum})')
app.shut()
plotter.plot()
