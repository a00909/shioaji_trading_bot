from datetime import datetime, timedelta
import pandas as pd

from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.order_placer import OrderPlacer
from tick_manager.history_tick_manager import HistoryTickManager
from tools.app import App
from tools.backtracking.dummy_rtm import DummyRealtimeTickManager
from tools.backtracking.dummy_shioaji import DummyShioaji


class LongBacktracker:
    def __init__(self):

        self.app = App(init=True)
        self.contract = self.app.api.Contracts.Futures.TMF.TMFR1
        self.account = self.app.api.futopt_account
        self.htm = HistoryTickManager(self.app.api, self.app.redis, self.app.session_maker)
        self.summery: dict[str, list] | None = None

    def _backtrack(self, date_str):
        print(f'processing: {date_str}')
        start = datetime.now()
        dummy_rtm = DummyRealtimeTickManager(self.contract, self.htm, self.app.redis, date_str)
        dummy_api = DummyShioaji(self.htm, dummy_rtm, self.account)
        op = OrderPlacer(dummy_api, self.contract, dummy_api.foutopt_account)
        ip = IndicatorProvider(dummy_rtm)
        strategy_runner = TMFStrategyRunner(self.htm, op, ip)
        strategy_runner.start()
        strategy_runner.wait_for_finish()

        profit_loss = dummy_api.list_profit_loss(None)

        if profit_loss:
            df = pd.DataFrame([item.dict() for item in profit_loss])
            pnl_sum = df["pnl"].sum()
            tax_sum = df["tax"].sum()
            fee_sum = df["fee"].sum()
            print("\nday pnl:", pnl_sum - tax_sum - fee_sum)
            print(f'total consume: {datetime.now() - start} (fee: {fee_sum}, tax: {tax_sum})')
            self._write_to_summery(pnl_sum, tax_sum, fee_sum, date_str)

    def _write_to_summery(self, pnl, tax, fee, date):
        if not self.summery:
            self.summery = {
                'date': [date],
                'net pnl': [pnl - tax - fee],
                'tax': [tax],
                'fee': [fee]
            }
        else:
            self.summery['date'].append(date)
            self.summery['net pnl'].append(pnl - tax - fee)
            self.summery['tax'].append(tax)
            self.summery['fee'].append(fee)

    def _show_summery(self):
        df_summery = pd.DataFrame(self.summery)
        print(df_summery)
        print(f'period pnl: {df_summery["net pnl"].sum()}')

    def start(self, start, end):
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')

        cur = start_dt
        while cur <= end_dt:
            self._backtrack(cur.strftime('%Y-%m-%d'))
            cur += timedelta(days=1)

        self.app.shut()
        self._show_summery()
