import logging
from datetime import timedelta, time

import numpy
import numpy as np
from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from strategy.tools.trailing_stop.trailing_stop_calculator import TrailingStopCalculator
from tools.plotter import plotter
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Ma Strategy')


class SdStopLossStrategy(AbsStrategy):

    def __init__(self, indicator_facade: IndicatorFacade):
        super().__init__(
            indicator_facade,
            [
                (time(8, 50), time(13, 30)),

                # (time(15, 15), time(4, 50)),
                # (time(20, 20), time(3, 00)),
                # (time(15, 15), time(4, 0))
            ]
        )
        self.last_direction = 0
        self.trailing_stop_tool = TrailingStopCalculator(lambda: self._donchian_width)

    @property
    def name(self):
        return "sd_stop_loss_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def _deal_with_trailing_stop(self, is_long):
        self.trailing_stop_tool.set_is_long(is_long)
        trailing_stop = self.trailing_stop_tool.calc_new_value(self._ma_p, 1, 30)
        plotter.add_points(
            'trailing_stop',
            (self.indicator_facade._ip.now, trailing_stop),
            chart_idx=0
        )
        return trailing_stop

    def _reset_trailing_stop(self):
        self.trailing_stop_tool.reset()
        plotter.add_points(
            'trailing_stop',
            (self.indicator_facade._ip.now, None),
            chart_idx=0
        )

    def _long_condition(self):
        return (
                self._donchian_breakthrough_h_s
                and self._donchian_hh_accumulation_s > 3
                and 2 > self._sell_buy_power > 1
                and self._donchian_hl_accumulation_s > 2
        )

    def _short_condition(self):
        return (
                self._donchian_breakthrough_l_s
                and self._donchian_ll_accumulation_s > 3
                and -2 < self._sell_buy_power < -1
                and self._donchian_lh_accumulation_s > 2
        )

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if (
                self._is_active_time
                and self._sd >= 6
                # and self._volume_ratio > 1.3
                # and self._vma_short > self._iiva*0.5
                # and self._vma_short > self._vma_long

        ):
            if self._short_condition():
                params = [
                    Action.Buy,
                ]
                self.last_direction = 1
                self._deal_with_trailing_stop(True)
            elif self._long_condition():
                params = [
                    Action.Sell,
                ]
                self.last_direction = -1
                self._deal_with_trailing_stop(False)
            else:
                pass

        res = self._get_report(params)

        return res

    def out_signal(self) -> StrategySuggestion | None:
        if not self.er:
            logger.info('no entry report yet.')
            return None

        params = None

        direction = 1 if self.er.action == Action.Buy else -1
        action_map = {1: [Action.Sell], -1: [Action.Buy]}

        trailing_stop = self._deal_with_trailing_stop(True if direction == 1 else False)

        if direction == 1 and (
                (self._ma_p - self.er.deal_price > self._sd * 4)
                or (self._ma_p - self.er.deal_price < -self._sd * 1.5)
        ):
            self._reset_trailing_stop()
            params = action_map[direction]

        elif direction == -1 and (
                (self._ma_p - self.er.deal_price < - self._sd * 4)
                or (self._ma_p - self.er.deal_price > self._sd * 1.5)
        ):
            self._reset_trailing_stop()
            params = action_map[direction]

        res = self._get_report(params, is_in=False)

        return res
