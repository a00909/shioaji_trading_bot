import logging
from datetime import timedelta, time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Period HL Strategy')


class DonchianStrategyTrend(AbsStrategy):

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

    @property
    def name(self):
        return "donchian_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if (
                self._is_active_time
                and self._sd >= 6
                # and self._volume_ratio > 0.5
                # and self._vma_short > self._iiva*0.5
                # and self._vma_short > self._vma_long

        ):

            if (
                    self._donchian_hh_accumulation > 1
                    and 0 < self._ma_p - self._donchian_h_25 < 55
                    and self._donchian_breakthrough_h
                    and self._sell_buy_ratio > 0
            ):
                params = [
                    Action.Buy,
                ]
            elif (
                    self._donchian_ll_accumulation > 1
                    and 0 < self._donchian_l_25 - self._ma_p < 55
                    and self._donchian_breakthrough_l
                    and self._sell_buy_ratio < 0
                    and False
            ):
                params = [
                    Action.Sell,
                ]
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
                    self._donchian_ll_accumulation_s > 0
                    and self._donchian_lh_accumulation_s > 0
                    or self._donchian_idle > 20

            ):
                params = action_map[direction]


        elif direction == -1:
            if (
                    self._donchian_hh_accumulation > 0
                    or self._sell_buy_ratio > 0
                    or self._ma_p > self._donchian_l_25
            ):
                params = action_map[direction]

        res = self._get_report(params, is_in=False)

        return res
