from collections import deque
from datetime import datetime, timedelta

from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.donchian import Donchian
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from tools.utils import get_by_time_range


class DonchianManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.DONCHIAN, length, symbol, start_time, redis, rtm)
        self.h_deque = deque()
        self.l_deque = deque()
        self.pivot_price_serial_counter = 0
        self.last_add_idle_time: datetime | None = None

    def calculate(self, now, last: Donchian):
        new = Donchian()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        right = now
        if last:
            left = last.datetime
        else:
            left = now - self.length

        ticks = self.rtm.get_ticks_by_time_range(left, right)
        h, l = self._deal(ticks)

        self._maintain_q(now, h, now - self.length, True)
        self._maintain_q(now, l, now - self.length, False)

        new.h = self.h_deque[0][1]
        new.l = self.l_deque[0][1]

        if last:


            if new.h > last.h:
                new.h_breakthrough = True
                new.hh_accumulation = last.hh_accumulation + 1
                new_lh_accu = 0
                new.idle_accumulation = 0
                self.last_add_idle_time = None
                if last.ll_accumulation > 0:
                    new.ll_accumulation = 0
                    new.pivot_price = h
                    new.pivot_price_changed = True
                    new.pivot_price_serial = self.pivot_price_serial_counter + 1
                    self.pivot_price_serial_counter += 1
            elif new.l < last.l:
                new.l_breakthrough = True
                new.ll_accumulation = last.ll_accumulation + 1
                new_hl_accu = 0
                new.idle_accumulation = 0
                self.last_add_idle_time = None
                if last.hh_accumulation > 0:
                    new.hh_accumulation = 0
                    new.pivot_price = l
                    new.pivot_price_changed = True
                    new.pivot_price_serial = self.pivot_price_serial_counter + 1
            else:
                if self.last_add_idle_time and new.datetime > self.last_add_idle_time + timedelta(seconds=1):
                    new.idle_accumulation = last.idle_accumulation + 1
                    self.last_add_idle_time = new.datetime

                new.ll_accumulation = last.ll_accumulation
                new.hh_accumulation = last.hh_accumulation
                new.pivot_price = last.pivot_price
                new.pivot_price_serial = self.pivot_price_serial_counter

            new_hl_accu = last.hl_accumulation
            new_lh_accu = last.lh_accumulation
            if new.l > last.l:
                new_hl_accu += 1
            elif new.l < last.l:
                new_hl_accu = 0
            if new.h < last.h:
                new_lh_accu += 1
            elif new.h > last.h:
                new_lh_accu = 0
            new.hl_accumulation = new_hl_accu
            new.lh_accumulation = new_lh_accu




        return new

    def _maintain_q(self, now: datetime, new_val, window_left: datetime, is_h):
        if is_h:
            q = self.h_deque
        else:
            q = self.l_deque

        # remove expired
        while q and q[0][0] < window_left:
            q.popleft()

        if is_h:
            while q and q[-1][1] <= new_val:
                q.pop()
        else:
            while q and q[-1][1] >= new_val:
                q.pop()

        q.append((now, new_val))

    def _calc_first(self, now):
        ticks = self.rtm.get_ticks_by_time_range(
            now - self.length,
            now,
        )

        return self._deal(ticks)

    def _deal(self, ticks):
        if not ticks:
            return None, None

        h = ticks[0].close
        l = ticks[0].close

        for t in ticks[1:]:
            if t.close > h:
                h = t.close
            if t.close < l:
                l = t.close

        return h, l
