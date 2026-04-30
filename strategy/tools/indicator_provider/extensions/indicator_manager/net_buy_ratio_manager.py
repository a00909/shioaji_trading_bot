from itertools import takewhile

from redis.client import Redis

from data.unified.tick.tick_fop import TickFOP
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.net_buy_ratio import NetBuyRatio
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager


class NetBuyRatioManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.NET_BUY_RATIO, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None


    def calculate(self, now, last: NetBuyRatio):
        new = NetBuyRatio()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            # 增量更新
            count, active_buy_vol, active_sell_vol = self._calc_incr(last, now)

        else:
            count, active_buy_vol, active_sell_vol = self._calc_first(now)

        new.active_buy_vol = active_buy_vol
        new.active_sell_vol = active_sell_vol
        new.data_count = count

        return new

    def _calc_first(self, now):
        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if len(ticks) == 0:
            raise Exception('no data to calculate!')

        # tick_type 定義：
        # 1 = 賣價成交（外盤）→ 買方主動 → 偏多
        # 2 = 買價成交（內盤）→ 賣方主動 → 偏空
        active_buy_vol = 0
        active_sell_vol = 0
        for t in ticks:
            if t.tick_type == 1:
                active_sell_vol += t.volume   # 外盤 = 買方主動
            elif t.tick_type == 2:
                active_buy_vol += t.volume  # 內盤 = 賣方主動

        self._collect_end_count(ticks)
        return len(ticks), active_buy_vol, active_sell_vol

    def _calc_incr(self, last: NetBuyRatio, now):
        # 增量更新

        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)

        a_count, a_active_buy_vol, a_active_sell_vol = self._deal_added_ticks(added_ticks)
        r_count, r_active_buy_vol, r_active_sell_vol = self._deal_removed_ticks(now, last)

        count = last.data_count + a_count - r_count
        active_buy_vol = last.active_buy_vol + a_active_buy_vol - r_active_buy_vol
        active_sell_vol = last.active_sell_vol + a_active_sell_vol - r_active_sell_vol

        self._collect_end_count(added_ticks)

        return count, active_buy_vol, active_sell_vol

    def _deal_added_ticks(self, added_ticks: list[TickFOP]):

        active_buy_vol = 0
        active_sell_vol = 0
        count = 0

        for e, t in enumerate(added_ticks):
            if t.datetime <= self.end_datetime and e < self.end_count:
                continue
            count+=1
            # tick_type 定義：
            # 1 = 賣價成交（外盤）→ 買方主動 → 偏多
            # 2 = 買價成交（內盤）→ 賣方主動 → 偏空
            if t.tick_type == 1:
                active_sell_vol += t.volume   # 外盤 = 買方主動
            elif t.tick_type == 2:
                active_buy_vol += t.volume  # 內盤 = 賣方主動

        return count, active_buy_vol, active_sell_vol

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

        active_buy_vol = 0
        active_sell_vol = 0

        if deprecated_ticks:
            for t in deprecated_ticks:
                # tick_type 定義：
                # 1 = 賣價成交（外盤）→ 買方主動 → 偏多
                # 2 = 買價成交（內盤）→ 賣方主動 → 偏空
                if t.tick_type == 1:
                    active_sell_vol += t.volume   # 外盤 = 買方主動
                elif t.tick_type == 2:
                    active_buy_vol += t.volume  # 內盤 = 賣方主動

        return len(deprecated_ticks), active_buy_vol, active_sell_vol

    def _collect_end_count(self, ticks: list[TickFOP]):
        if not ticks:
            self.end_count = 0
            return

        last_dt = ticks[-1].datetime
        reversed_ticks = reversed(ticks)

        end_count = sum(1 for _ in takewhile(lambda tick: tick.datetime >= last_dt, reversed_ticks))

        self.end_count = end_count
        self.end_datetime = last_dt
