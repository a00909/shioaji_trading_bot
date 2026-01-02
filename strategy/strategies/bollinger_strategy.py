import logging
from datetime import timedelta, time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Ma Strategy')


class BollingerStrategy(AbsStrategy):

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
        self._fixed_stop_loss = 0.0

    @property
    def name(self):
        return "bollinger_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if (
                self._is_active_time
                and self._sd > 1.2
                and self._volume_ratio <= 1.5
                # and self._vma_short > self._iiva*0.5
                # and self._vma_short > self._vma_long

        ):
            if (
                    not self._sell_buy_ratio < -0.5
                    and self._price < self._ma_l - self._bb_width * 0.5
                    and self._price - (self._bb_lower - self._sd * 2) < 55
            ):
                params = [
                    Action.Buy,
                ]
                self._fixed_stop_loss = self._bb_lower - self._sd * 2
            elif (
                    not self._sell_buy_ratio > 0.5
                    and self._price > self._ma_l + self._bb_width * 0.5
                    and (self._bb_lower + self._sd * 2) - self._price < 55
            ):
                params = [
                    Action.Sell,
                ]
                self._fixed_stop_loss = self._bb_upper + self._sd * 2
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
            if self._price < self._fixed_stop_loss:
                params = action_map[direction]
            elif self._price > self._ma_l + self._bb_width * 0.5:
                if (
                        self._price < self._sd_stop_loss
                        or (self._sell_buy_ratio < -0.2 and self._sell_buy_ratio_change_rate < -0.2)
                ):
                    params = action_map[direction]

        elif direction == -1:
            if                    self._price > self._fixed_stop_loss:
                params = action_map[direction]
            elif self._price < self._ma_l - self._bb_width * 0.5:
                if (
                        self._price > self._sd_stop_loss
                        or (self._sell_buy_ratio > 0.2 and self._sell_buy_ratio_change_rate > 0.2)
                ):
                    params = action_map[direction]


        res = self._get_report(params, is_in=False)

        return res
