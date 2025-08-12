from abc import abstractmethod
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, datetime

from data.bid_ask_fop_v1d1 import BidAskFOPv1D1
from data.tick_fop_v1d1 import TickFOPv1D1


class RealtimeTickManagerBase:
    def __init__(self):

        self.tick_right = -1
        self.tick_left = 0
        self.tick_buffer: list[TickFOPv1D1] = []

        self.bid_ask_right = -1
        self.bid_ask_left = 0
        self.bid_ask_buffer: list[BidAskFOPv1D1] = []

        self.buffer_clean_limit = timedelta(hours=3)
        self.window_size = timedelta(hours=2)

    def update_window(self):
        self.update_window_right()
        self._update_window_left()

    def _update_window_left(self):
        self.tick_left = self._update_window_detail(
            self.tick_buffer,
            self.tick_left,
            self.tick_right
        )
        self.bid_ask_left = self._update_window_detail(
            self.bid_ask_buffer,
            self.bid_ask_left,
            self.bid_ask_right
        )

    @staticmethod
    def _valid(buffer, left, right):
        return len(buffer) and right <= len(buffer)

    def _update_window_detail(self, buffer, left, right):
        if not self._valid(buffer, left, right):
            return

        if buffer[left].datetime < buffer[right].datetime - self.buffer_clean_limit:
            while buffer[left].datetime < buffer[right].datetime - self.window_size:
                left += 1
        return left

    @abstractmethod
    def update_window_right(self):
        pass

    def get_ticks_by_time_range(self, start: datetime, end: datetime, with_start=True, with_end=True) -> list[
        TickFOPv1D1]:

        return self._get_by_time_range(
            self.tick_buffer,
            self.tick_left,
            self.tick_right + 1,
            start,
            end,
            with_start,
            with_end
        )

    def get_bidask_by_time_range(self, start: datetime, end: datetime, with_start=True, with_end=True) -> list[
        BidAskFOPv1D1]:
        return self._get_by_time_range(
            self.bid_ask_buffer,
            self.bid_ask_left,
            self.bid_ask_right + 1,
            start,
            end,
            with_start,
            with_end
        )

    def _get_by_time_range(
            self,
            buffer,
            lo,
            hi,
            start: datetime,
            end: datetime,
            with_start=True,
            with_end=True
    ) -> list[TickFOPv1D1 | BidAskFOPv1D1]:
        ranges = (lo, hi)
        if not self._valid(buffer, lo, hi):
            return []

        if with_start:
            left = bisect_left(buffer, start, *ranges)
        else:
            left = bisect_right(buffer, start, *ranges)

        if with_end:
            right = bisect_right(buffer, end, *ranges)
        else:
            right = bisect_left(buffer, end, *ranges)

        return buffer[left:right]

    def latest_tick(self):
        return self.tick_buffer[self.tick_right]

    def prev_tick(self):
        if self.tick_right - self.tick_left + 1 >= 2 and self.tick_right >= 0:
            return self.tick_buffer[self.tick_right-1]

    def latest_bidask(self):
        return self.bid_ask_buffer[self.bid_ask_right]

    @abstractmethod
    def wait_for_tick(self):
        pass
