from collections import deque

from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.change_rate import ChangeRate
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from tools.utils import get_by_time_range


class PeriodHLManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm, indicator_manager: AbsIndicatorManager):
        super().__init__(IndicatorType.INDICATOR_CHANGE_RATE, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None
        self.indicator_manager: AbsIndicatorManager = indicator_manager
        self.deque = deque()

    def calculate(self, now, last: ChangeRate):
        new = ChangeRate()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last and False:
            rsum, tsum, rtsum, rsqsum, count = self._calc_incr(last, now)
        else:
            rsum, tsum, rtsum, rsqsum, count = self._calc_first(now)

        new.rsum = rsum
        new.tsum = tsum
        new.rtsum = rtsum
        new.rsqsum = rsqsum
        new.data_count = count

        return new

    def _calc_first(self, now):
        ticks = self.rtm.get_ticks_by_time_range(
            now - self.length,
            now,
        )

        self._collect_end_count()
        return self._deal(ticks)

    def _calc_incr(self, last: ChangeRate, now):
        # 增量更新

        a_vals = self._deal_added_ticks(last.datetime, now)
        r_vals = self._deal_removed_ticks(now, last.datetime)
        last_vals = (last.rsum, last.tsum, last.rtsum, last.rsqsum, last.data_count)

        rsum, tsum, rtsum, rsqsum, count = (
            l + a - r for l, a, r in zip(last_vals, a_vals, r_vals)
        )

        self._collect_end_count()

        return rsum, tsum, rtsum, rsqsum, count

    def _deal_added_ticks(self, left, right):

        ticks = self.rtm.get_ticks_by_time_range(left, right)

        p = 0
        while p < len(ticks):
            if ticks[p].datetime <= self.end_datetime and p < self.end_count:
                p += 1
            else:
                break

        return self._deal(ticks[p:])

    def _deal_removed_ticks(self, now, last_datetime):
        if now == last_datetime:
            return 0, 0, 0, 0, 0
        elif now < last_datetime:
            raise Exception('now < last.datetime!')
        else:
            ticks = self.rtm.get_ticks_by_time_range(
                last_datetime - self.length,
                now - self.length,
                with_end=False,
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

    def _collect_end_count(self):
        if not self.indicator_manager.buffer:
            self.end_count = 0
            return

        count = 0
        lastest = self.indicator_manager.get(return_indicator=True)
        last_dt = lastest.datetime
        p = len(self.indicator_manager.buffer) - 1
        while p >= 0 and self.indicator_manager.get(p, return_indicator=True).datetime == last_dt:
            p -= 1
            count += 1

        self.end_count = count
        self.end_datetime = last_dt
