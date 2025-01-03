from abc import abstractmethod
from bisect import bisect_left, bisect_right
from datetime import timedelta, datetime

from data.tick_fop_v1d1 import TickFOPv1D1


class RealtimeTickManagerBase:
    def __init__(self):

        self.window_right = -1
        self.window_left = 0
        self.buffer: list[TickFOPv1D1] = []
        self.window_size = timedelta(hours=3)

    def _update_window_left(self):
        while self.buffer[self.window_left].datetime < self.buffer[self.window_right].datetime - self.window_size:
            self.window_left += 1

    def get_ticks_by_time_range(self, start: datetime, end: datetime, with_start=True, with_end=True) -> list[
        TickFOPv1D1]:
        cur_range = self.buffer[:self.window_right + 1]

        if with_start:
            left = bisect_left(cur_range, start)
        else:
            left = bisect_right(cur_range, start)

        if with_end:
            right = bisect_right(cur_range, end)
        else:
            right = bisect_left(cur_range, end)

        return cur_range[left:right]

    def latest_tick(self):
        return self.buffer[self.window_right]

    @abstractmethod
    def wait_for_tick(self):
        pass