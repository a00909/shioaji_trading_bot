import datetime
import line_profiler_pycharm
import threading
import time
from enum import Enum

from line_profiler_pycharm import profile
from shioaji.constant import OrderState, Action
from shioaji.order import Trade
from shioaji.position import FuturePosition, StockPosition

from strategy.runner.abs_strategy_runner import AbsStrategyRunner
from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import EntryReport
from strategy.strategies.ma_stragegy import MaStrategy
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import get_now


class TMFStrategyRunner(AbsStrategyRunner):

    def __init__(self, rtm, htm, op):
        super().__init__(rtm, htm, op)
        self.trades = None
        self.strategies: list[AbsStrategy] = []
        self.start_time = None
        self.unit = datetime.timedelta(seconds=5)
        self.chk_timedelta = datetime.timedelta(minutes=180)
        self.finish = threading.Event()

        # self.default_max_position = 3
        # self.stop_lost = 0
        # self.take_profit = 999999

    def init_strategies(self):

        ma_stra = MaStrategy(self.ip, self.chk_timedelta)

        # add more strategies
        self.strategies.append(ma_stra)

    def prepare(self):
        self.init_strategies()
        self.update_positions()
        self.start_time = get_now()

    def print_indicators(self):
        # indicator part
        latest_price = self.ip.latest_price()

        ma = self.ip.ma(self.chk_timedelta)
        is_increasing, slope = self.ip.is_increasing(self.chk_timedelta)

        vol_avg, va_msg = self.ip.vol_avg(self.chk_timedelta, self.unit, with_msg=True)
        vol_avg_short, sva_msg = self.ip.vol_avg(datetime.timedelta(seconds=30), self.unit, with_msg=True)

        atr = self.ip.atr(self.chk_timedelta, self.unit)

        print(
            f'[Indicators]\n'
            f'| newest price: {latest_price}\n'
            f'| {self.chk_timedelta.total_seconds()}_s_ma: {ma} | slope: {slope} | is_increasing: {is_increasing}\n'
            f'| atr: {atr}\n'
            f'{va_msg}\n'
            f'{sva_msg}\n'
        )

    def wait_for_finish(self):
        self.finish.wait()

    @profile
    def strategy_loop(self):

        self.prepare()

        cur_stra_idx = -1

        while self.run:
            if not self.ip.wait_for_tick():
                break

            self.print_indicators()

            if cur_stra_idx >= 0:
                suggest = self.strategies[cur_stra_idx].out_signal()
                if suggest and suggest.valid:
                    self.order_placer.place_order_by_suggestion(suggest)
                    self.order_placer.wait_for_completely_deal()
                    self.update_positions()
                    cur_stra_idx = -1
            else:
                for e, stra in enumerate(self.strategies):
                    suggest = stra.in_signal()
                    if suggest and suggest.valid:
                        # self.print_indicators()
                        cur_stra_idx = e
                        self.order_placer.place_order_by_suggestion(suggest)
                        self.order_placer.wait_for_completely_deal()
                        self.update_positions()

                        last_deal = self.order_placer.get_last_deal_info()

                        qty = 0
                        cover_price = 0
                        for d in last_deal:
                            qty += d['quantity']
                            cover_price += d['price']
                        cover_price /= len(last_deal)

                        er = EntryReport()
                        er.deal_time = datetime.datetime.fromtimestamp(last_deal[0]['ts']).replace(tzinfo=DEFAULT_TIMEZONE)
                        er.deal_price = cover_price
                        er.quantity = qty
                        er.action = Action.Buy if last_deal[0]['action'] == 'Buy' else Action.Sell

                        stra.report_entry(er)

            self.ip.clear_lru_cache()

            # time.sleep(2)

        self.finish.set()

    def get_cover_price(self, action: Action):
        ps: list[FuturePosition | StockPosition] | None = None
        if action == Action.Buy:
            ps = self.long_positions
        elif action == Action.Sell:
            ps = self.short_positions
        else:
            raise Exception('Invalid action!')

        return sum(p.price for p in ps) / len(ps)

    def order_callback(self, state: OrderState, msg: dict):
        pass

    # cbs
    def order_cb(self, trade: Trade):
        print(f'order callback:\n{trade}')

    # def chk_trade_cb(self, trades: list[Trade]):
    #     print(f'chk_trade callback:\n{trades}')
    #     self.trades = trades
    #     total_qty = 0
    #     for t in trades:
    #         total_qty += t.order.quantity
    #
    #     print(f'total quantity: {total_qty}, will be close.')
    #     self.order_placer.simple_sell(total_qty, self.order_cb)
