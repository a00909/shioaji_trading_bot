import datetime
import threading

from line_profiler_pycharm import profile
from numpy.f2py.auxfuncs import isintent_overwrite
from shioaji.constant import OrderState, Action
from shioaji.order import Trade
from shioaji.position import FuturePosition, StockPosition

from strategy.runner.abs_strategy_runner import AbsStrategyRunner
from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.bollinger_strategy import BollingerStrategy
from strategy.strategies.data import EntryReport
from strategy.strategies.donchian_strategy import DonchianStrategyTrend
from strategy.strategies.donchian_strategy_swing import DonchianStrategySwing
from strategy.strategies.extensions.donchian_swing_state_memorizer import DonchianSwingStateMemorizer
from strategy.strategies.ma_stragegy import MaStrategy
from strategy.strategies.period_hl_strategy import PeriodHLStrategy
from strategy.strategies.period_hl_strategy_trend import PeriodHLStrategyTrend
from strategy.strategies.sd_stop_loss_strategy import SdStopLossStrategy
from strategy.strategies.trend_strategy import TrendStrategy
from strategy.strategies.volume_strategy import VolumeStrategy
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from tools.constants import DEFAULT_TIMEZONE
from tools.plotter import plotter
from tools.ui_signal_emitter import ui_signal_emitter
from tools.utils import get_now


class TMFStrategyRunner(AbsStrategyRunner):
    MAX_LOSS = -1000
    MAX_TRADE_TIMES = 30
    def __init__(self, htm, op, ip, print_indicators=True):
        super().__init__(htm, op, ip)
        self.trades = None
        self.strategies: list[AbsStrategy] = []
        self.start_time = None

        self.finish = threading.Event()
        self.indicator_facade = IndicatorFacade(ip)
        self.indicator_state_memorizers = []

        self.init_strategies()
        self.is_print_indicators = print_indicators

        # self.default_max_position = 3
        # self.stop_lost = 0
        # self.take_profit = 999999

    def init_strategies(self):
        donchian_indicator_state_memorizer = DonchianSwingStateMemorizer(self.indicator_facade)

        self.indicator_state_memorizers.append(donchian_indicator_state_memorizer)

        ma_stra = MaStrategy(self.indicator_facade)
        vol_stra = VolumeStrategy(self.indicator_facade)
        trend_stra = TrendStrategy(self.indicator_facade)
        sd_stop_loss_stra = SdStopLossStrategy(self.indicator_facade)
        bb_stra = BollingerStrategy(self.indicator_facade)
        period_hl_stra = PeriodHLStrategyTrend(self.indicator_facade)
        donchian_trend = DonchianStrategyTrend(self.indicator_facade)
        donchian_swing = DonchianStrategySwing(self.indicator_facade, donchian_indicator_state_memorizer)

        # add more strategies
        # hint: sequence matters
        # self.strategies.append(vol_stra)
        # self.strategies.append(ma_stra)
        # self.strategies.append(trend_stra)
        # self.strategies.append(bb_stra)
        self.strategies.append(sd_stop_loss_stra)
        # self.strategies.append(period_hl_stra)
        # self.strategies.append(donchian_trend)
        # self.strategies.append(donchian_swing)

    def prepare(self):
        self.update_positions()
        self.start_time = get_now()

    def get_strategy_active_time_ranges(self):
        ranges = []
        for s in self.strategies:
            ranges.extend(s.active_time_ranges)
        return ranges

    def print_indicators(self):
        # indicator part
        if not self.is_print_indicators:
            return
        facade_lists = [
            [
                # self.indicator_facade.latest_price,
                self.indicator_facade.pma_p,
                # # self.indicator_facade.pma_l,
                self.indicator_facade.pma_m,
                self.indicator_facade.pma_s,
                self.indicator_facade.donchian_h,
                self.indicator_facade.donchian_l,
                self.indicator_facade.donchian_h_25,
                self.indicator_facade.donchian_l_25,
                self.indicator_facade.donchian_h_s,
                self.indicator_facade.donchian_l_s,
            ],
            # [
            #     self.indicator_facade.donchian_hh_accumulation,
            #     self.indicator_facade.donchian_ll_accumulation,
            #     self.indicator_facade.donchian_hh_accumulation_s,
            #     self.indicator_facade.donchian_ll_accumulation_s,
            # ],
            # [
            #     self.indicator_facade.donchian_idle,
            #     self.indicator_facade.donchian_hl_accumulation_s,
            #     self.indicator_facade.donchian_lh_accumulation_s,
            # ],
            [
                self.indicator_facade.sell_buy_power
            ],
            [
                self.indicator_facade.volume_ratio,
                # self.indicator_facade.vma_short,
                # self.indicator_facade.iiva_l30d_i5m,
            ],
            [
                self.indicator_facade.sell_buy_ratio,
                # self.indicator_facade.sell_buy_ratio_change_rate,
            ],
        ]

        msg = f'[Indicators]\n'
        for e, sublist in enumerate(facade_lists):
            for facade_unit in sublist:
                msg += f'{facade_unit.msg}\n'

                plotter.add_points(
                    facade_unit.name,
                    (self.ip.now, facade_unit()),
                    chart_idx=e
                )

        # if self.indicator_facade.period_pivot_price_changed():
        #     plotter.add_points(
        #         self.indicator_facade.period_pivot_price.name,
        #         (self.ip.now,self.indicator_facade.period_pivot_price()),
        #         point_only=True,
        #         point_text=f'{self.indicator_facade.period_pivot_price_serial()}'
        #     )

        ui_signal_emitter.emit_indicator(msg)
        # print(msg)

    def wait_for_finish(self):
        self.finish.wait()
        self.order_placer.close_all()

    def _update_indicator_state_memorizers(self):
        for i in self.indicator_state_memorizers:
            i.update()

    def _deal_entry(self):
        for e, stra in enumerate(self.strategies):
            entry_suggest = stra.in_signal()
            if entry_suggest and entry_suggest.valid:
                # self.print_indicators() # todo: remove after test
                cur_stra_idx = e
                self.order_placer.place_order_by_suggestion(entry_suggest)
                self.order_placer.wait_for_completely_deal()
                self.update_positions()

                last_deal = self.order_placer.get_last_deal_info()

                qty = 0
                cover_price = 0
                numbers = []
                for d in last_deal:
                    qty += d['quantity']
                    cover_price += d['price']
                    numbers.append(d['seqno'])
                cover_price /= len(last_deal)

                er = EntryReport()
                er.deal_time = datetime.datetime.fromtimestamp(last_deal[0]['ts']).replace(
                    tzinfo=DEFAULT_TIMEZONE)
                er.deal_price = cover_price
                er.quantity = qty
                er.action = Action.Buy if last_deal[0]['action'] == 'Buy' else Action.Sell

                stra.report_entry(er)

                direction = 'long' if entry_suggest.action == Action.Buy else 'short'
                plotter.add_points(
                    f'{direction}_open',
                    (self.ip.now, self.ip.latest_price()),
                    point_only=True,
                    point_text=",".join(map(str, numbers))
                )
                return cur_stra_idx
        return -1

    def _deal_exit(self, cur_stra_idx):
        out_suggest = self.strategies[cur_stra_idx].out_signal()
        is_over_loss = False

        if out_suggest and out_suggest.valid:
            # self.print_indicators() # todo: remove after test
            self.order_placer.place_order_by_suggestion(out_suggest)
            self.order_placer.wait_for_completely_deal()
            self.update_positions()
            cur_stra_idx = -1

            last_deals = self.order_placer.get_last_deal_info()
            numbers = []
            for d in last_deals:
                numbers.append(d['seqno'])

            direction = 'long' if out_suggest.action == Action.Sell else 'short'
            plotter.add_points(
                f'{direction}_close',
                (self.ip.now, self.ip.latest_price()),
                point_only=True,
                point_text=",".join(map(str, numbers)),
                down=False,
            )
            is_over_loss = self._should_stop()

        return cur_stra_idx, is_over_loss

    def _should_stop(self):
        profit_loss = self.order_placer.api.list_profit_loss(None)
        net = 0
        for p in profit_loss:
            net += p.pnl - p.fee - p.tax
        if net <= self.MAX_LOSS:
            return True
        if len(profit_loss) >= self.MAX_TRADE_TIMES:
            return True        
        return False

    @profile
    def strategy_loop(self):
        print('tmf strategy runner loop started.')

        self.prepare()

        cur_stra_idx = -1

        while self.run:
            if not self.ip.wait_for_update():
                break

            self.print_indicators()
            self._update_indicator_state_memorizers()

            if cur_stra_idx >= 0:
                cur_stra_idx, is_over_loss = self._deal_exit(cur_stra_idx)
                if is_over_loss:
                    break
            else:
                cur_stra_idx = self._deal_entry()

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
