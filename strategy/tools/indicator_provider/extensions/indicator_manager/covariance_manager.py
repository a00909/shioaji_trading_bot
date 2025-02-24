from itertools import takewhile
from typing_extensions import override

from redis.client import Redis

from data.tick_fop_v1d1 import TickFOPv1D1
from strategy.tools.indicator_provider.extensions.data.covariance import Covariance
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from tools.utils import deviation, error


class CovarianceManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.COVARIANCE, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None

    @override
    def get(self, backward_idx=-1, value_only=True):
        indicator: Covariance = super().get(backward_idx, value_only=False)

        if not indicator:
            return None

        if value_only:
            covariance = (
                    indicator.spt / indicator.data_count
                    - (indicator.sp * indicator.st / indicator.data_count ** 2)
            )
            return covariance
        else:
            return indicator

    def calculate(self, now, last: Covariance):
        new = Covariance()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            # 增量更新
            count, sp, st, spt = self._calc_incr(last, now)

            # for test
            # count_o, sp_o, st_o, spt_o = self._calc_first(now)
            # error(count_o, count,0)
            # error(sp_o, sp)
            # error(st_o, st)
            # error(spt_o, spt)

        else:
            count, sp, st, spt = self._calc_first(now)

        new.data_count = count
        new.sp = sp
        new.st = st
        new.spt = spt

        return new

    def _calc_first(self, now):
        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if len(ticks) == 0:
            raise Exception('no data to calculate!')

        count = sum(tick.volume for tick in ticks)
        p_sum = sum(tick.close * tick.volume for tick in ticks)
        t_sum = sum(tick.datetime.timestamp() * tick.volume for tick in ticks)
        pt_sum = sum(tick.close * tick.datetime.timestamp() * tick.volume for tick in ticks)

        self._collect_end_count(ticks)
        return count, p_sum, t_sum, pt_sum

    def _calc_incr(self, last: Covariance, now):
        # 增量更新

        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)

        a_count, a_sp, a_st, a_spt = self._deal_added_ticks(added_ticks)
        r_count, r_sp, r_st, r_spt = self._deal_removed_ticks(now, last)

        count = last.data_count - r_count + a_count
        sp = last.sp - r_sp + a_sp
        st = last.st - r_st + a_st
        spt = last.spt - r_spt + a_spt

        self._collect_end_count(added_ticks)

        return count, sp, st, spt

    def _deal_added_ticks(self, added_ticks):

        sp = 0
        st = 0
        spt = 0
        count = 0

        for e, t in enumerate(added_ticks):
            if t.datetime <= self.end_datetime and e < self.end_count:
                continue
            sp += t.close * t.volume
            st += t.datetime.timestamp() * t.volume
            spt += t.close * t.datetime.timestamp() * t.volume
            count += t.volume

        return count, sp, st, spt

    def _deal_removed_ticks(self, now, last):
        if now == last.datetime:
            deprecated_ticks = []
        elif now < last.datetime:
            raise Exception('now < last.datetime!')
        else:
            deprecated_ticks = self.rtm.get_ticks_by_time_range(
                last.datetime - self.length,
                now - self.length,
                with_end=False
            )

        count = 0
        sp = 0
        st = 0
        spt = 0

        if deprecated_ticks:
            for t in deprecated_ticks:
                count += t.volume
                sp += t.close * t.volume
                st += t.datetime.timestamp() * t.volume
                spt += t.datetime.timestamp() * t.close * t.volume

        return count, sp, st, spt

    def _collect_end_count(self, ticks: list[TickFOPv1D1]):
        if not ticks:
            self.end_count = 0
            return

        last_dt = ticks[-1].datetime
        reversed_ticks = reversed(ticks)

        end_count = sum(1 for _ in takewhile(lambda tick: tick.datetime >= last_dt, reversed_ticks))

        self.end_count = end_count
        self.end_datetime = last_dt
