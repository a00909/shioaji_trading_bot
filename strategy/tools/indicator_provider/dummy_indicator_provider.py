from datetime import time
from typing_extensions import override

from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.utils import is_in_time_ranges


class DummyIndicatorProvider(IndicatorProvider):
    def __init__(self, rtm, kbar_indicator_center):
        super().__init__(rtm, kbar_indicator_center)
        self._active_time_ranges: list[tuple[time, time]] = None

    def set_active_time_ranges(self, active_ranges):
        self._active_time_ranges = active_ranges

    @override
    def update(self):
        if self._active_time_ranges and is_in_time_ranges(self.now.time(), self._active_time_ranges):
            super().update()
