import logging
from datetime import timedelta, time
from enum import Enum

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.strategies.extensions.donchian_swing_state_memorizer import DonchianSwingStateMemorizer
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Period HL Strategy')


class DonchianStrategySwing(AbsStrategy):
    # states
    L_25 = 0
    H_25 = 1
    M = 2

    def __init__(self, indicator_facade: IndicatorFacade, state_memorizer):
        super().__init__(
            indicator_facade,
            [
                (time(8, 50), time(13, 30)),

                # (time(15, 15), time(4, 50)),
                # (time(20, 20), time(3, 00)),
                # (time(15, 15), time(4, 0))
            ]
        )
        self._states = []
        self._sm: DonchianSwingStateMemorizer = state_memorizer

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
                and self._volume_ratio < 1

        ):

            if (
                    (
                            self._sm.l25() and self._donchian_hh_accumulation_s > 2
                            or self._sm.up_cross_l25() and self._donchian_hh_accumulation_s > 1
                    )

            ):
                params = [
                    Action.Buy,
                ]
            elif (
                    False
                    and
                    (
                            self._sm.h25() and self._donchian_ll_accumulation_s > 6
                            or self._sm.down_cross_h25() and self._donchian_ll_accumulation_s > 5
                    )

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
                    self._donchian_breakthrough_l
                    or
                    (
                            (
                                    self._sm.h25() or self._sm.down_cross_h25()
                            )
                            and self._donchian_ll_accumulation_s > 1
                    )
            ):
                params = action_map[direction]

        elif direction == -1:
            if (
                    self._donchian_breakthrough_h
                    or
                    (
                            (
                                    self._sm.l25() or self._sm.up_cross_l25()
                            )
                            and self._donchian_hh_accumulation_s > 1
                    )
            ):
                params = action_map[direction]

        res = self._get_report(params, is_in=False)

        return res
