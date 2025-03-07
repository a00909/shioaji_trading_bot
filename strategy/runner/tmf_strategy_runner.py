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
from strategy.strategies.volume_strategy import VolumeStrategy
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from tools.constants import DEFAULT_TIMEZONE
from tools.plotter import plotter
from tools.ui_signal_emitter import ui_signal_emitter
from tools.utils import get_now


class TMFStrategyRunner(AbsStrategyRunner):

    def __init__(self, htm, op, ip):
        super().__init__(htm, op, ip)
        self.trades = None
        self.strategies: list[AbsStrategy] = []
        self.start_time = None

        self.finish = threading.Event()
        self.indicator_facade = IndicatorFacade(ip)

        # self.default_max_position = 3
        # self.stop_lost = 0
        # self.take_profit = 999999

    def init_strategies(self):


        ma_stra = MaStrategy(self.indicator_facade)
        vol_stra = VolumeStrategy(self.indicator_facade)

        # add more strategies
        # hint: sequence matters
        # self.strategies.append(vol_stra)
        self.strategies.append(ma_stra)

    def prepare(self):
        self.init_strategies()
        self.update_positions()
        self.start_time = get_now()

    def print_indicators(self):
        # indicator part
        facade_lists = [
            [
                self.indicator_facade.latest_price,
                self.indicator_facade.pma_long,
                self.indicator_facade.pma_short,
                self.indicator_facade.bb_upper,
                self.indicator_facade.bb_lower,
            ],
            [
                self.indicator_facade.vma_long,
                self.indicator_facade.vma_short,
            ],
            [
                self.indicator_facade.covariance_long,
                self.indicator_facade.covariance_short,
            ]
        ]

        msg = f'[Indicators]\n'
        for e,sublist in enumerate(facade_lists):
            for facade_unit in sublist:
                msg += f'{facade_unit.msg}\n'

                plotter.add_points(
                    facade_unit.name,
                    (self.ip.now,facade_unit()),
                    chart_idx=e
                )

        ui_signal_emitter.emit_indicator(msg)
        # print(msg)



    def wait_for_finish(self):
        self.finish.wait()
        self.order_placer.close_all()

    @profile
    def strategy_loop(self):
        print('tmf strategy runner loop started.')

        self.prepare()

        cur_stra_idx = -1

        while self.run:
            if not self.ip.wait_for_update():
                break

            self.print_indicators()

            if cur_stra_idx >= 0:
                suggest = self.strategies[cur_stra_idx].out_signal()
                if suggest and suggest.valid:
                    # self.print_indicators() # todo: remove after test
                    self.order_placer.place_order_by_suggestion(suggest)
                    self.order_placer.wait_for_completely_deal()
                    self.update_positions()
                    cur_stra_idx = -1

                    direction = 'long' if suggest.action == Action.Sell else 'short'
                    plotter.add_points(f'{direction}_close', (self.ip.now, self.ip.latest_price()), point_only=True)
            else:
                for e, stra in enumerate(self.strategies):
                    suggest = stra.in_signal()
                    if suggest and suggest.valid:
                        # self.print_indicators() # todo: remove after test
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
                        er.deal_time = datetime.datetime.fromtimestamp(last_deal[0]['ts']).replace(
                            tzinfo=DEFAULT_TIMEZONE)
                        er.deal_price = cover_price
                        er.quantity = qty
                        er.action = Action.Buy if last_deal[0]['action'] == 'Buy' else Action.Sell

                        stra.report_entry(er)

                        direction = 'long' if suggest.action == Action.Buy else 'short'
                        plotter.add_points(f'{direction}_open', (self.ip.now, self.ip.latest_price()), point_only=True)
                        break

            # time.sleep(2)
        print('tmf strategy runner loop stopped.')
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
