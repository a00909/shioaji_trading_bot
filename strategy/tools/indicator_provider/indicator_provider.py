from datetime import timedelta
from functools import lru_cache

from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.vma_manager import VMAManager
from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager


class IndicatorProvider:

    def __init__(self, rtm):
        self.rtm: RealtimeTickManager = rtm
        self.redis = self.rtm.redis
        self.now = None
        self.indicator_managers: dict[
            tuple[IndicatorType, timedelta] |
            tuple[IndicatorType, timedelta, timedelta],
            AbsIndicatorManager
        ] = {}

    def start(self):
        self.rtm.start(wait_for_ready=True)

    def stop(self):
        self.rtm.stop()
        for m in self.indicator_managers.values():
            m.dump_to_redis(anyway=True)

        print('ip stopped.')

    def latest_price(self):
        return self.rtm.latest_tick().close

    def wait_for_update(self):
        valid = self.rtm.wait_for_tick()
        if valid:
            self.now = self.rtm.latest_tick().datetime
            self.update()
        return valid

    def update(self):
        for m in self.indicator_managers.values():
            m.update(self.now)

    def ma(self, length: timedelta):
        key = (IndicatorType.PMA, length)

        if key in self.indicator_managers:
            return self.indicator_managers[(IndicatorType.PMA, length)]()

        im = PMAManager(
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im.update(self.now)
        self.indicator_managers[key] = im
        return im()

    def slope(self, short, long):
        short_ma = self.ma(short)
        long_ma = self.ma(long)
        return short_ma - long_ma


    def vol_avg(self, length: timedelta, unit: timedelta, with_msg=False) -> tuple[
                                                                                 float, str] | float:
        key = (IndicatorType.VMA, length, unit)
        if key in self.indicator_managers:
            if with_msg:
                return self.indicator_managers[key](), self.indicator_managers[key].msg
            return self.indicator_managers[key]()

        im = VMAManager(
            length,
            unit,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im.update(self.now)
        self.indicator_managers[key] = im
        if with_msg:
            return im(), im.msg
        return im()

    @lru_cache
    def atr(self, length: timedelta, unit: timedelta):
        """
        計算 ATR (Average True Range) 基於固定的時間單位
        :param length: ATR 計算的總時間長度
        :param unit: ATR 分段計算的時間單位
        :return: ATR 值
        """

        ticks = self.rtm.get_ticks_by_time_range(self.now - length, self.now)
        if len(ticks) == 0:
            return -1

        end = ticks[-1].datetime
        start = ticks[0].datetime
        amounts = int(length.total_seconds() / unit.total_seconds())

        counter = 0
        tick_counter = 0
        trs = []

        while counter < amounts:

            # 設置初始最高價和最低價
            h = ticks[tick_counter].close
            l = ticks[tick_counter].close

            # 定義當前區間的結束時間
            unit_end = start + unit

            while tick_counter < len(ticks) and ticks[tick_counter].datetime < unit_end:
                h = max(h, ticks[tick_counter].close)
                l = min(l, ticks[tick_counter].close)
                tick_counter += 1

            # 如果有有效的 ticks，記錄 TR 值
            if h != l:
                trs.append(h - l)

            start += unit
            counter += 1

            if tick_counter >= len(ticks):
                break

        if not trs:  # 如果沒有計算出任何有效的 TR，避免除以零
            return -1

        atr = sum(float(tr) for tr in trs) / len(trs)
        return atr

    def clear_lru_cache(self):
        self.atr.cache_clear()
