from datetime import timedelta
from functools import lru_cache

from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager
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
        self.provider_indicator_managers: dict[
            tuple[IndicatorType, timedelta] |
            tuple[IndicatorType, timedelta, timedelta],
            AbsIndicatorManager
        ] = {}
        self.client_indicator_managers: dict[
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
        for m in self.provider_indicator_managers.values():
            m.update(self.now)

        for m in self.client_indicator_managers.values():
            m.update(self.now)

    def _get_or_new_indicator(self, key, cls, params):
        if key in self.indicator_managers:
            return self.indicator_managers[key]

        im = cls(*params)
        im.update(self.now)
        self.indicator_managers[key] = im
        self.client_indicator_managers[key] = im
        return im

    def ma(self, length: timedelta, value_only=True):
        key = (IndicatorType.PMA, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )

        im = self._get_or_new_indicator(key, PMAManager, params)
        if value_only:
            return im.get()
        return im

    def vol_avg(self, length: timedelta, unit: timedelta, with_msg=False) \
            -> tuple[float, str] | float:
        key = (IndicatorType.VMA, length, unit)
        params = (
            length,
            unit,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, VMAManager, params)

        if with_msg:
            return im.get(), im.msg
        return im.get()

    def standard_deviation(self, length: timedelta, unit: timedelta):

        # ma_manager = self.ma(length, value_only=False)
        key = (IndicatorType.SD, length, unit)
        params = (
            length,
            unit,
            self.rtm,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            # ma_manager
        )

        im = self._get_or_new_indicator(key, StandardDeviationManager, params)
        # ma_key = (IndicatorType.PMA, length)
        # if ma_key in self.client_indicator_managers:
        #     self.provider_indicator_managers[ma_key] = ma_manager
        #     del self.client_indicator_managers[ma_key]

        return im.get()

    def slope(self, short, long):
        short_ma = self.ma(short)
        long_ma = self.ma(long)
        return short_ma - long_ma
