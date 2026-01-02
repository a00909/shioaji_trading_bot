from datetime import timedelta

from redis.client import Redis

from data.unified.tick.tick_fop import TickFOP
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType


class VMAManager(AbsIndicatorManager):
    def __init__(self, length, unit, symbol: str, start_time, redis: Redis, rtm, with_msg=False):
        super().__init__(IndicatorType.VMA, length, symbol, start_time, redis, rtm)
        self.unit: timedelta = unit
        self.with_msg = with_msg
        self.msg: str = None
        self.last_start = None  # msg用
        self.end_values = None  # 紀錄當時點可能未完整的資料
        self.end_count = None

    def calculate(self, now, last):
        new = Indicator()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length
        intervals = int(self.length.total_seconds() / self.unit.total_seconds())

        if last:
            data_count, value = self._calc_incr(now, last, intervals)
        else:
            data_count, value = self._calc_first(now, intervals)

        new.data_count = data_count
        new.value = value

        return new

    def _calc_first(self, now, intervals):
        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if len(ticks) == 0:
            raise Exception('no data to calculate!')

        data_count = len(ticks)
        value = sum(tick.volume for tick in ticks) / intervals

        self.collect_end_count(ticks)

        if self.with_msg:
            # test
            # org_count, org_value = self._calc_first(now, intervals)
            # self._update_msg(now, value, data_count, self.last_start, ticks[-1].datetime, org_value, org_count)

            self._update_msg(now, value, data_count, self.last_start, ticks[-1].datetime)
            self.last_start = ticks[0].datetime

        return data_count, value

    def _calc_incr(self, now, last, intervals):
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
            deprecated_val = sum(t.volume for t in deprecated_ticks) + self.end_values

        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)

        added_val = sum(t.volume for t in added_ticks)

        data_count = last.data_count + len(added_ticks) - len(deprecated_ticks) - self.end_count
        value = (last.value * intervals + added_val - deprecated_val) / intervals
        self.collect_end_count(added_ticks)

        if self.with_msg:
            start = deprecated_ticks[-1].datetime if deprecated_ticks else self.last_start
            end = added_ticks[-1].datetime if added_ticks else None
            self._update_msg(now, value, data_count, start, end)
            self.last_start = start

        return data_count, value

    def _update_msg(self, now, value, count, start, end, org_avg=None, org_count=None):
        if not self.with_msg:
            return

        msg = (
            f'[Vol avg info (l={self.length.total_seconds()} s)]\n'
            f'start: {(now - self.length).strftime("%H:%M:%S")} | end: {now.strftime("%H:%M:%S")}\n'
            f'| avg: {value} | per: {self.unit} | delta:{(end - start if end and start else "n/a")}\n'
            f'| {count} ticks'
        )
        if org_avg and org_count:
            msg += f' | org_avg: {org_avg} | org_count: {org_count}'

        self.msg = msg

    def collect_end_count(self, ticks: list[TickFOP]):
        last_dt = ticks[-1].datetime
        p = len(ticks) - 1
        end_values = 0
        while ticks[p].datetime >= last_dt and p >= 0:
            end_values += ticks[p].volume
            p -= 1
        self.end_values = end_values
        self.end_count = len(ticks) - 1 - p
