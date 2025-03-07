import logging
from datetime import time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade

logger = logging.getLogger('Volume Strategy')


class VolumeStrategy(AbsStrategy):

    def __init__(self, indicator_facade: IndicatorFacade):
        super().__init__(
            indicator_facade,
            [
                (time(8, 50), time(13, 40)),
                (time(15, 5), time(4, 0))
            ]
        )
        self.covariance_threshold = 5000

    @property
    def name(self):
        return "volume_strategy"

    def _report_entry_detail(self, er: EntryReport):
        pass

    def in_signal(self) -> StrategySuggestion | None:
        params = None
        if self._is_high_volume and self._is_active_time:
            if (
                    self._covariance_long >= self.covariance_threshold and
                    self._covariance_short > 0 and
                    self._price >= self._ma + self._sd
            ):
                params = [Action.Buy]
            elif (
                    self._covariance_long <= -self.covariance_threshold and
                    self._covariance_short < 0 and
                    self._price <= self._ma - self._sd
            ):
                params = [Action.Sell]

        res = self._get_report(params)
        return res

    def out_signal(self) -> StrategySuggestion | None:
        if not self.er:
            logger.info('no entry report yet.')
            return None

        params = None
        direction = 1 if self.er.action == Action.Buy else -1
        action_map = {1: [Action.Sell], -1: [Action.Buy]}

        # 固定止損
        if (self.er.deal_price - self._price) * direction >= self.enter_sd:
            params = action_map[direction]

        # 協方差止損
        if self._covariance_long * direction <= 0:
            params = action_map[direction]

        # # 固定止盈
        # if (self._price - self.er.deal_price) * direction >= max(self._sd, 100):
        #     params = action_map[direction]

        # 滾動止損
        if self.stop_loss:
            if (self._price - self.stop_loss) * direction >= 0.5 * self._sd:
                self.stop_loss += direction * 0.25 * self._sd
            else:
                params = action_map[direction]
        elif (self._price - self.er.deal_price) * direction >= self._sd:
            self.stop_loss = self._price - 0.25 * self._sd * direction

        res = self._get_report(params, is_in=False)
        return res
