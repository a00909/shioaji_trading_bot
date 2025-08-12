from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta, datetime
from functools import lru_cache

from line_profiler_pycharm import profile

from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.bid_ask_diff_manager import BidAskDiffManager
from strategy.tools.indicator_provider.extensions.indicator_manager.bid_ask_ratio_ma_manager import BidAskRatioMaManager
from strategy.tools.indicator_provider.extensions.indicator_manager.covariance_manager import CovarianceManager
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sd_stopsloss_manager import SDStopLossManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sell_buy_diff_manager import SellBuyDiffManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager
from strategy.tools.indicator_provider.extensions.indicator_manager.vma_manager import VMAManager
from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager


class IndicatorProvider:

    def __init__(self, rtm):
        self.rtm: RealtimeTickManager = rtm
        self.redis = self.rtm.redis
        self.now: datetime | None = None

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

        self.tpe = ThreadPoolExecutor(max_workers=8)

    def start(self):
        self.rtm.start(wait_for_ready=True)

    def stop(self):
        self.rtm.stop()
        for m in self.indicator_managers.values():
            m.dump_to_redis(anyway=True)

        print('ip stopped.')

    def latest_price(self):
        return self.rtm.latest_tick().close

    @profile
    def wait_for_update(self):
        valid = self.rtm.wait_for_tick()
        if valid:
            self.now = self.rtm.latest_tick().datetime
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
        futures_provider = [self.tpe.submit(m.update, self.now) for m in self.provider_indicator_managers.values()]

        # 等待所有provider指标管理器的任务完成
        for future in futures_provider:
            try:
                future.result()  # 获取结果，如果有异常会在这里抛出
            except Exception as e:
                print(f"发生异常：{e}")

        # 提交client指标管理器的更新任务到线程池中（等待上一步完成后）
        futures_client = [self.tpe.submit(m.update, self.now) for m in self.client_indicator_managers.values()]

        # 等待所有client指标管理器的任务完成（可选）
        for future in futures_client:
            try:
                future.result()  # 获取结果，如果有异常会在这里抛出
            except Exception as e:
                print(f"发生异常：{e}")

    def _get_or_new_indicator(self, key, cls=None, params=None, is_provider=False):
        if key in self.indicator_managers:
            return self.indicator_managers[key]

        elif params and cls:
            im = cls(*params)
            im.update(self.now)
            self.indicator_managers[key] = im

            if is_provider:
                self.provider_indicator_managers[key] = im
            else:
                self.client_indicator_managers[key] = im
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

    def vma(self, length: timedelta, unit: timedelta, times=1) \
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

    def sell_buy_diff(self, length):
        key = (IndicatorType.SELL_BUY_DIFF, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, SellBuyDiffManager, params)
        return im.get()

    def bid_ask_diff(self):
        ba = self.rtm.latest_bidask()

        return ba.bid_volume - ba.ask_volume

    def bid_ask_diff_ma(self, length):
        key = (IndicatorType.BID_ASK_DIFF, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, BidAskDiffManager, params)
        return im.get()

    def bid_ask_ratio_ma(self, length):
        key = (IndicatorType.BID_ASK_RATIO_MA, length)
        params = (
            length,
            self.rtm.symbol,
            self.rtm.start_time,
            self.redis,
            self.rtm
        )
        im = self._get_or_new_indicator(key, BidAskRatioMaManager, params)
        return im.get()

    def sd_stop_loss(self, sd_length, pma_length):
        key = (IndicatorType.BID_ASK_RATIO_MA, sd_length, pma_length)

        im = self._get_or_new_indicator(key)

        if not im:
            sd_manager = self.standard_deviation(sd_length, get_manager=True)
            pma_manager = self.ma(pma_length, get_manager=True)

            if sd_manager and pma_manager:
                params = (
                    sd_length,
                    pma_length,
                    self.rtm,
                    self.rtm.symbol,
                    self.rtm.start_time,
                    self.redis, sd_manager,
                    pma_manager
                )
                im = self._get_or_new_indicator(key, SDStopLossManager, params)

        return im.get()