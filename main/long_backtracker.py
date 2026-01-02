from datetime import datetime, timedelta, time
import pandas as pd
from shioaji.position import FutureProfitLoss

from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from strategy.tools.indicator_provider.dummy_indicator_provider import DummyIndicatorProvider
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.kbar_indicators.kbar_indicator_center import KbarIndicatorCenter
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
        self.total_profit_losses: list[FutureProfitLoss] = []

    def _backtrack(self, date_str, active_time_ranges: list[tuple[time, time]]):
        print(f'processing: {date_str}')
        start = datetime.now()
        dummy_rtm = DummyRealtimeTickManager(self.contract, self.htm, self.app.redis, date_str, active_time_ranges)
        dummy_api = DummyShioaji(self.htm, dummy_rtm, self.account)
        op = OrderPlacer(dummy_api, self.contract, dummy_api.foutopt_account)
        ip = DummyIndicatorProvider(
            dummy_rtm,
            KbarIndicatorCenter(self.contract, self.app.api, self.app.redis, self.app.session_maker)
        )
        if active_time_ranges:
            ip.set_active_time_ranges(active_time_ranges)
        strategy_runner = TMFStrategyRunner(self.htm, op, ip, False)
        strategy_runner.start()
        strategy_runner.wait_for_finish()

        profit_losses:list[FutureProfitLoss] = dummy_api.list_profit_loss(None)

        if profit_losses:
            df = pd.DataFrame([item.dict() for item in profit_losses])
            pnl_sum = df["pnl"].sum()
            tax_sum = df["tax"].sum()
            fee_sum = df["fee"].sum()
            print("\nday pnl:", pnl_sum - tax_sum - fee_sum)
            print(f'total consume: {datetime.now() - start} (fee: {fee_sum}, tax: {tax_sum})')
            self._write_to_summery(pnl_sum, tax_sum, fee_sum, date_str)
            self.total_profit_losses.extend(profit_losses)

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
        with pd.option_context(
                'display.max_rows', None,
                'display.max_columns', None
        ):
            df_summery = pd.DataFrame(self.summery)
            print(df_summery)
            print(f'period pnl: {df_summery["net pnl"].sum()}')

    def _show_analyzing(self):
        t_profits = []
        t_losses = []
        for pl in self.total_profit_losses:
            net = pl.pnl - pl.tax -pl.fee
            if net > 0:
                t_profits.append(net)
            if net < 0:
                t_losses.append(net)

        win_rate_percentage = len(t_profits)/(len(t_losses)+len(t_profits))*100
        pl_ratio = (sum(t_profits)/len(t_profits))/(sum(t_losses)/len(t_losses)) * -1

        print(f'{len(self.total_profit_losses)} trades.')
        print(f'win rate: {win_rate_percentage}%')
        print(f'pl ratio: {pl_ratio}')



    def start(self, start, end, active_time_ranges: list[tuple[time, time]] = None):
        start_dt = datetime.strptime(start, '%Y-%m-%d')
        end_dt = datetime.strptime(end, '%Y-%m-%d')

        cur = start_dt
        while cur <= end_dt:
            self._backtrack(cur.strftime('%Y-%m-%d'), active_time_ranges)
            cur += timedelta(days=1)

        self.app.shut()
        self._show_summery()
        self._show_analyzing()
