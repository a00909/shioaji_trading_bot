import logging
from datetime import timedelta, time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Period HL Strategy')


class PeriodHLStrategy(AbsStrategy):

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
        self.fixed_stop_loss = 0
        self.fixed_take_profit = 0
        self.pl_ratio = 1.5

    @property
    def name(self):
        return "period_hl_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if (
                self._is_active_time
                and self._sd >= 6
                and self._volume_ratio < 1.5
                and not (self._donchian_breakthrough_h or self._donchian_breakthrough_l)
                # and self._vma_short > self._iiva*0.5
                # and self._vma_short > self._vma_long

        ):

            if (
                    self._ma_s > self._donchian_h_25
                    and self._donchian_h - self._ma_s < 50
                    and self._sell_buy_ratio <-0.02

            ):
                params = [
                    Action.Sell,
                ]
                self.last_direction = -1
                self.fixed_stop_loss = self._donchian_h + self._sd
                self.fixed_take_profit = self._price - (self.fixed_stop_loss - self._price) * self.pl_ratio
            elif (
                    self._ma_s < self._donchian_l_25
                    and self._ma_s - self._donchian_l < 50
                    and self._sell_buy_ratio > 0.02
            ):
                params = [
                    Action.Buy,
                ]
                self.last_direction = 1
                self.fixed_stop_loss = self._donchian_l - self._sd
                self.fixed_take_profit = self._price + (self._price - self.fixed_stop_loss) * self.pl_ratio
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

        if direction == 1:
            if (
                    self._ma_s < self.fixed_stop_loss
                    or
                    (
                            self._ma_s > self.fixed_take_profit
                    )
                    or self._price >= self._donchian_h
            ):
                params = action_map[direction]


        elif direction == -1:
            if (
                    self._ma_s > self.fixed_stop_loss
                    or
                    (
                            self._ma_s < self.fixed_take_profit
                    )
                    or self._price <= self._donchian_l
            ):
                params = action_map[direction]

        res = self._get_report(params, is_in=False)

        return res
