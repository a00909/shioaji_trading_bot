from datetime import timedelta
from decimal import Decimal

from redis.client import Redis

from data.tick_fop_v1d1 import TickFOPv1D1
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType


class VMAIndicatorManager(AbsIndicatorManager):
    def __init__(self, length, unit, symbol: str, start_time, redis: Redis, rtm, with_msg=True):
        super().__init__(IndicatorType.VMA, length, symbol, start_time, redis, rtm)
        self.unit: timedelta = unit
        self.with_msg = with_msg
        self.msg: str = None
        self.last_start = None
        self.end_values = None  # 紀錄當時點可能未完整的資料
        self.end_count = None

    def calculate(self, now, last):
        new = Indicator()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length
        intervals = int(self.length.total_seconds() / self.unit.total_seconds())
        org_avg = None
        org_count = None

        # 增量更新
        if last:
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

            # print(
            #     last.data_count,
            #     self.end_count,
            #     deprecated_val,
            #     len(deprecated_ticks),
            #     added_val,
            #     len(added_ticks),
            # )

            new.data_count = last.data_count + len(added_ticks) - len(deprecated_ticks) - self.end_count
            new.value = (last.value * intervals + added_val - deprecated_val) / intervals
            self.collect_end_count(added_ticks)

            if self.with_msg:
                start = deprecated_ticks[-1].datetime if deprecated_ticks else self.last_start
                end = added_ticks[-1].datetime if added_ticks else None
                tick_count = new.data_count
                self.last_start = start

            # for test
            ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
            if len(ticks) == 0:
                raise Exception('no data to calculate!')
            org_avg = sum(tick.volume for tick in ticks) / intervals
            org_count = len(ticks)

            if org_count and org_count != tick_count or (org_avg - new.value) / org_avg > 0.001:
                raise Exception(f'Incorrect value! {org_count},{tick_count} | {org_avg},{new.value}')

        else:
            ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
            if len(ticks) == 0:
                raise Exception('no data to calculate!')

            new.data_count = len(ticks)
            new.value = sum(tick.volume for tick in ticks) / intervals

            end = ticks[-1].datetime
            self.collect_end_count(ticks)

            if self.with_msg:
                start = ticks[0].datetime
                end = ticks[-1].datetime
                tick_count = len(ticks)
                self.last_start = start



        if self.with_msg:
            self.msg = (
                f'[Vol avg info (l={self.length.total_seconds()} s)]\n'
                f'start: {(now - self.length).strftime("%H:%M:%S")} | end: {now.strftime("%H:%M:%S")}\n'
                f'| avg: {new.value} | per: {self.unit} | delta:{(end - start if end and start else "n/a")}\n'
                f'| {tick_count} ticks | org_avg: {org_avg} | org_count: {org_count}'
            )

        return new

    def collect_end_count(self, ticks: list[TickFOPv1D1]):
        last_dt = ticks[-1].datetime
        p = len(ticks) - 1
        end_values = 0
        while ticks[p].datetime >= last_dt and p >= 0:
            end_values += ticks[p].volume
            p -= 1
        self.end_values = end_values
        self.end_count = len(ticks) - 1 - p
