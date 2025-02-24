from abc import abstractmethod
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime

from data.tick_fop_v1d1 import TickFOPv1D1


class RealtimeTickManagerBase:
    def __init__(self):

        self.window_right = -1
        self.window_left = 0
        self.buffer: list[TickFOPv1D1] = []
        self.buffer_clean_limit = timedelta(hours=3)
        self.window_size = timedelta(hours=2)

    def _update_window_left(self):
        if self.buffer[self.window_left].datetime < self.buffer[self.window_right].datetime - self.buffer_clean_limit:
            while self.buffer[self.window_left].datetime < self.buffer[self.window_right].datetime - self.window_size:
                self.window_left += 1

    def _update_window_left_bisect(self):
        if self.buffer[self.window_left].datetime < self.buffer[self.window_right].datetime - self.buffer_clean_limit:
            self.window_left = bisect_left(
                self.buffer,
                self.buffer[self.window_right].datetime - self.window_size,
                self.window_left,
                self.window_right + 1
            )

    def get_ticks_by_time_range(self, start: datetime, end: datetime, with_start=True, with_end=True) -> list[
        TickFOPv1D1]:
        # cur_range = self.buffer[:self.window_right + 1]

        lo = self.window_left
        hi = self.window_right + 1
        ranges = (lo, hi)

        if with_start:
            left = bisect_left(self.buffer, start, *ranges)
        else:
            left = bisect_right(self.buffer, start, *ranges)

        if with_end:
            right = bisect_right(self.buffer, end, *ranges)
        else:
            right = bisect_left(self.buffer, end, *ranges)

        return self.buffer[left:right]

    def latest_tick(self):
        return self.buffer[self.window_right]

    @abstractmethod
    def wait_for_tick(self):
        pass
