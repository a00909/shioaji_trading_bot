from itertools import takewhile

from redis.client import Redis

from data.unified.tick.tick_fop import TickFOP
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.sell_buy_ratio import SellBuyRatio
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager


class SellBuyRatioManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.SELL_BUY_RATIO, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None


    def calculate(self, now, last: SellBuyRatio):
        new = SellBuyRatio()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            # 增量更新
            count, sell, buy = self._calc_incr(last, now)

        else:
            count, sell, buy = self._calc_first(now)

        new.sell = sell
        new.buy = buy
        new.data_count = count

        return new

    def _calc_first(self, now):
        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if len(ticks) == 0:
            raise Exception('no data to calculate!')

        sell = 0
        buy = 0
        for t in ticks:
            if t.tick_type == 1:
                sell += t.volume
            elif t.tick_type == 2:
                buy += t.volume

        self._collect_end_count(ticks)
        return len(ticks), sell, buy

    def _calc_incr(self, last: SellBuyRatio, now):
        # 增量更新

        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)

        a_count, a_sell, a_buy = self._deal_added_ticks(added_ticks)
        r_count, r_sell, r_buy = self._deal_removed_ticks(now, last)

        count = last.data_count + a_count - r_count
        sell = last.sell + a_sell - r_sell
        buy = last.buy + a_buy - r_buy

        self._collect_end_count(added_ticks)

        return count, sell, buy

    def _deal_added_ticks(self, added_ticks: list[TickFOP]):

        sell = 0
        buy = 0
        count = 0

        for e, t in enumerate(added_ticks):
            if t.datetime <= self.end_datetime and e < self.end_count:
                continue
            count+=1
            if t.tick_type == 1:
                sell += t.volume
            elif t.tick_type == 2:
                buy += t.volume

        return count, sell, buy

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

        sell = 0
        buy = 0

        if deprecated_ticks:
            for t in deprecated_ticks:
                if t.tick_type == 1:
                    sell += t.volume
                elif t.tick_type == 2:
                    buy += t.volume

        return len(deprecated_ticks), sell, buy

    def _collect_end_count(self, ticks: list[TickFOP]):
        if not ticks:
            self.end_count = 0
            return

        last_dt = ticks[-1].datetime
        reversed_ticks = reversed(ticks)

        end_count = sum(1 for _ in takewhile(lambda tick: tick.datetime >= last_dt, reversed_ticks))

        self.end_count = end_count
        self.end_datetime = last_dt
