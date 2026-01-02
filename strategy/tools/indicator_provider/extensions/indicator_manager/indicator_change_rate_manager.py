from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.change_rate import ChangeRate
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from tools.utils import get_by_time_range


class IndicatorChangeRateManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm, indicator_manager: AbsIndicatorManager):
        super().__init__(IndicatorType.INDICATOR_CHANGE_RATE, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None
        self.indicator_manager: AbsIndicatorManager = indicator_manager

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
        left, right = get_by_time_range(
            self.indicator_manager.buffer,
            0,
            len(self.indicator_manager.buffer),
            now - self.length,
            now,
            index_only=True
        )

        self._collect_end_count()
        return self._deal(left, right)

    def _calc_incr(self, last: ChangeRate, now):
        # 增量更新
        left, right = get_by_time_range(
            self.indicator_manager.buffer,
            0,
            len(self.indicator_manager.buffer),
            last.datetime,
            now,
            index_only=True
        )

        a_rsum, a_tsum, a_rtsum, a_rsqsum, a_count = self._deal_added_ticks(left, right)
        r_rsum, r_tsum, r_rtsum, r_rsqsum, r_count = self._deal_removed_ticks(now, last)

        rsum = last.rsum + a_rsum - r_rsum
        tsum = last.tsum + a_tsum - r_tsum
        rtsum = last.rtsum + a_rtsum - r_rtsum
        rsqsum = last.rsqsum + a_rsqsum - r_rsqsum
        count = last.data_count + a_count - r_count

        self._collect_end_count()

        return rsum, tsum, rtsum, rsqsum, count

    def _deal(self, left, right):
        count = right - left
        rsum = 0
        tsum = 0
        rtsum = 0
        rsqsum = 0

        p = left
        while p < right:
            i = self.indicator_manager.get(p, return_indicator=True)

            r = i.get()
            t = i.datetime.timestamp() - self.start_time.timestamp()

            rsum += r
            tsum += t
            rtsum += r * t
            rsqsum += r ** 2

            p += 1

        return rsum, tsum, rtsum, rsqsum, count

    def _deal_added_ticks(self, left, right):
        p = left
        while p < right:
            i = self.indicator_manager.get(p, return_indicator=True)
            if i.datetime <= self.end_datetime and p - left - 1 < self.end_count:
                p += 1
            else:
                break

        return self._deal(p, right)

    def _deal_removed_ticks(self, now, last):
        if now == last.datetime:
            return 0, 0, 0, 0, 0
        elif now < last.datetime:
            raise Exception('now < last.datetime!')
        else:
            l, r = get_by_time_range(
                self.indicator_manager.buffer,
                0,
                len(self.indicator_manager.buffer),
                last.datetime - self.length,
                now - self.length,
                with_end=False,
                index_only=True
            )

        return self._deal(l, r)

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
