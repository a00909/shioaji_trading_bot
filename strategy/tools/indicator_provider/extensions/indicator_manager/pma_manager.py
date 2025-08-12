from redis.client import Redis

from data.tick_fop_v1d1 import TickFOPv1D1
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType


class PMAManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.PMA, length, symbol, start_time, redis, rtm)
        self.end_values = None
        self.end_count = None
        self.diff = None

    def calculate(self, now, last):
        new = Indicator()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            # 增量更新
            data_count, value = self._calc_incr(last, now)
            # 計算差值
            self.diff = value - last.value
        else:
            data_count, value = self._calc_first(now)

        new.data_count = data_count
        new.value = value

        return new

    def _calc_first(self, now) -> tuple[int, float]:
        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if len(ticks) == 0:
            raise Exception(f'no data to calculate! query range: ({now-self.length},{now}), buffer size: {len(self.rtm.tick_buffer)}')

        data_count = sum(tick.volume for tick in ticks)
        value = sum(tick.close * tick.volume for tick in ticks) / data_count
        self._collect_end_count(ticks)
        return data_count, value

    def _calc_incr(self, last, now) -> tuple[int, float]:
        # 增量更新

        if now == last.datetime:
            deprecated_ticks = []
            deprecated_val = self.end_values
        elif now < last.datetime:
            raise Exception('now < last.datetime!')
        else:
            deprecated_ticks = self.rtm.get_ticks_by_time_range(
                last.datetime - self.length,
                now - self.length,
                with_end=False
            )
            deprecated_val = sum(t.close * t.volume for t in deprecated_ticks) + self.end_values

        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)
        added_val = sum(t.close * t.volume for t in added_ticks)

        data_count = (
                last.data_count
                + sum(t.volume for t in added_ticks)
                - sum(t.volume for t in deprecated_ticks)
                - self.end_count
        )
        value = (last.value * last.data_count + added_val - deprecated_val) / data_count

        self._collect_end_count(added_ticks)

        # for test
        # org_data_count, org_value = self._calc_first(now)
        # if org_data_count != data_count or (org_value-value)/org_value >= 0.001:
        #     raise Exception(f'Incorrect value! {org_data_count},{data_count} | {org_value},{value}')

        return data_count, value

    def _collect_end_count(self, ticks: list[TickFOPv1D1]):
        last_dt = ticks[-1].datetime
        p = len(ticks) - 1
        end_values = 0
        end_count = 0
        while ticks[p].datetime >= last_dt and p >= 0:
            end_values += ticks[p].close * ticks[p].volume
            end_count += ticks[p].volume
            p -= 1

        self.end_values = end_values
        self.end_count = end_count


