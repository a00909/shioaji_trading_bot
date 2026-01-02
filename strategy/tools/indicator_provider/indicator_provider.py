import traceback
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta, datetime

from fontTools.ufoLib.utils import deprecated
from line_profiler_pycharm import profile

from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.donchian import Donchian
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.indicator_manager.bid_ask_ratio_manager import BidAskRatioManager
from strategy.tools.indicator_provider.extensions.indicator_manager.covariance_manager import CovarianceManager
from strategy.tools.indicator_provider.extensions.indicator_manager.donchian_manager import DonchianManager
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sd_stopsloss_manager import SDStopLossManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sell_buy_ratio_manager import SellBuyRatioManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager
from strategy.tools.indicator_provider.extensions.indicator_manager.vma_manager import VMAManager
from strategy.tools.kbar_indicators.kbar_indicator_center import KbarIndicatorCenter
from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager
from tools.utils import get_twse_date


class IndicatorProvider:
    DEFAULT_UPDATE_INTERVAL_MILLISECOND = 1000

    def __init__(self, rtm, kbar_indicator_center):
        self.rtm: RealtimeTickManager = rtm
        self.redis = self.rtm.redis
        self.now: datetime | None = None

        self.indicator_managers: dict[tuple, AbsIndicatorManager] = {}

        self.indicator_hierarchy: list[list] = []

        self.tpe = ThreadPoolExecutor(max_workers=8)

        self.kbar_indicator_center: KbarIndicatorCenter = kbar_indicator_center

        self._last_update: datetime | None = None

    def start(self):
        self.rtm.start(wait_for_ready=True)

    def stop(self):
        self.rtm.stop()
        for m in self.indicator_managers.values():
            m.dump_to_redis(anyway=True)

        print('ip stopped.')

    def latest_price(self):
        return self.rtm.latest_tick().close

    def _is_time_to_update(self):
        if 0 == self.DEFAULT_UPDATE_INTERVAL_MILLISECOND:
            return True

        if self._last_update:
            if self.now - self._last_update > timedelta(milliseconds=self.DEFAULT_UPDATE_INTERVAL_MILLISECOND):
                self._last_update = self.now
                return True
            else:
                return False
        else:
            self._last_update = self.now
            return True

    @profile
    def wait_for_update(self):
        valid = self.rtm.wait_for_tick()
        if valid:
            self.now = self.rtm.latest_tick().datetime
            if self._is_time_to_update():
                self.update()
        return valid

    # def update(self):
    #     for m in self.provider_indicator_managers.values():
    #         m.update(self.now)
    #
    #     for m in self.client_indicator_managers.values():
    #         m.update(self.now)

    @profile
    def update(self):
        # 提交provider指标管理器的更新任务到线程池中

        for level in self.indicator_hierarchy:
            futures = [self.tpe.submit(m.update, self.now) for m in level]

            for future in futures:
                try:
                    future.result()  # 获取结果，如果有异常会在这里抛出
                except Exception as e:
                    print(f"發生錯誤: {traceback.format_exc()}")

    def _get_or_new_indicator(self, key, cls=None, params=None, level_idx=0):
        # level 0 for 無依賴指標，有依賴則往後遞加
        if key in self.indicator_managers:
            return self.indicator_managers[key]

        elif params and cls:
            im = cls(*params)
            im.update(self.now)
            self.indicator_managers[key] = im

            not_enough = level_idx + 1 - len(self.indicator_hierarchy)
            if not_enough > 0:
                for _ in range(not_enough):
                    self.indicator_hierarchy.append([])

            self.indicator_hierarchy[level_idx].append(im)
            return im
        else:
            return None

    def ma(self, length: timedelta, get_manager=False):
        key = (IndicatorType.PMA, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )

        im = self._get_or_new_indicator(key, PMAManager, params)

        return im if get_manager else im.get()

    def vma(self, length: timedelta, unit: timedelta, times=1, get_manager=False) \
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

        if get_manager:
            return im
        return im.get() * times

    def standard_deviation(self, length: timedelta, get_manager=False):

        # ma_manager = self.ma(length, value_only=False)
        key = (IndicatorType.SD, length)
        params = (
            length,
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

        return im if get_manager else im.get()

    def covariance(self, length):
        key = (IndicatorType.COVARIANCE, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, CovarianceManager, params)
        return im.get()

    def sell_buy_ratio(self, length, get_manager=False):
        key = (IndicatorType.SELL_BUY_RATIO, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, SellBuyRatioManager, params)
        if get_manager:
            return im
        return im.get()

    def bid_ask_diff(self):
        ba = self.rtm.latest_bidask()

        return ba.bid_volume - ba.ask_volume

    def bid_ask_ratio(self, length):
        key = (IndicatorType.BID_ASK_RATIO, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, BidAskRatioManager, params)
        return im.get()

    @deprecated
    def sd_stop_loss(
            self,
            sd_length,
            pma_length,
            vma_length,
            vma_unit,
            sell_buy_ratio_length,
            sell_buy_ratio_change_rate_length,
            iiva_length,
            iiva_interval=5,
    ):
        key = (
            IndicatorType.SD_STOP_LOSS,
            f'{sd_length}_{vma_length}_{vma_unit}_{sell_buy_ratio_length}_{iiva_length}_{iiva_interval}'
        )

        im = self._get_or_new_indicator(key)

        if not im:
            sd_manager = self.standard_deviation(sd_length, get_manager=True)
            vma_manager = self.vma(vma_length, vma_unit, get_manager=True)
            sell_buy_ratio_manager = self.sell_buy_ratio(sell_buy_ratio_length, get_manager=True)
            pma_manager = self.ma(pma_length,get_manager=True)

            if sd_manager and vma_manager:
                params = (
                    self.rtm, self.rtm.symbol, self.rtm.start_time, self.redis,
                    sd_manager,
                    sd_length,
                    vma_manager,
                    self.kbar_indicator_center,
                    iiva_length,
                    iiva_interval,
                    sell_buy_ratio_manager,
                    sell_buy_ratio_change_rate_length,
                    pma_manager
                )
                im = self._get_or_new_indicator(key, SDStopLossManager, params, level_idx=1)

        return im.get()

    def donchian(self, length):
        key = (IndicatorType.DONCHIAN, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, DonchianManager, params)

        period_hl: Donchian = im.get(return_indicator=True)

        return period_hl

        # def sell_buy_ratio_change_rate(self, length):

    #     key = (IndicatorType.INDICATOR_CHANGE_RATE, IndicatorType.SELL_BUY_RATIO, length)
    #     im = self._get_or_new_indicator(key)
    #
    #     if not im:
    #         sell_buy_diff_manager = self.sell_buy_ratio(length, get_manager=True)
    #
    #         if sell_buy_diff_manager:
    #             params = (
    #                 length,
    #                 self.rtm.symbol,
    #                 self.rtm.start_time,
    #                 self.rtm.redis,
    #                 self.rtm, sell_buy_diff_manager
    #             )
    #             im = self._get_or_new_indicator(key, IndicatorChangeRateManager, params)
    #
    #     return im.get()

    def sell_buy_ratio_change_rate(self, length, change_rate_length=None):
        im = self.sell_buy_ratio(length, get_manager=True)
        if not change_rate_length:
            change_rate_length = length
        return im.change_rate(change_rate_length)

    def intraday_interval_volume_avg(self, length, interval_min):
        twse_date = get_twse_date(self.now)

        val = self.kbar_indicator_center.iiva.get(
            self.now.replace(year=twse_date.year, month=twse_date.month, day=twse_date.day),
            length,
            interval_min
        )
        return val
