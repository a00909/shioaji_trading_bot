import logging
from datetime import timedelta, time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Ma Strategy')


class MaStrategy(AbsStrategy):

    def __init__(self, indicator_facade: IndicatorFacade):
        super().__init__(
            indicator_facade,
            [
                (time(9, 0), time(13, 30)),
                (time(15, 15), time(4, 0))
            ]
        )

    @property
    def name(self):
        return "ma_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if (
                self._is_active_time and
                not self._is_high_volume and
                self._sd > 6 and
                abs(self._covariance_long) < 10000 and
                self._vma_long < 200
        ):
            if self._price <= self._ma - self._sd * 1.5 and self._covariance_long > 0:
                params = [
                    Action.Buy,
                ]
            elif self._price >= self._ma + self._sd * 1.5 and self._covariance_long < 0:
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

        if (self.er.deal_price - self._price) * direction >= self._sd * 2:
            params = action_map[direction]

        if self.stop_loss:
            if (self._price - self.stop_loss) * direction >= 0.5 * self._sd:
                self.stop_loss += direction * 0.25 * self._sd
            else:
                params = action_map[direction]

        elif (self._price - self.er.deal_price) * direction >= self._sd:
            self.stop_loss = self._price - 0.25 * self._sd * direction

        res = self._get_report(params, is_in=False)

        return res
