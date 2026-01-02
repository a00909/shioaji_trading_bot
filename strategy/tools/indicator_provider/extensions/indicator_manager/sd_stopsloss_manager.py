import math

from fontTools.ufoLib.utils import deprecated
from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.sd_stop_loss import SdStopLoss
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sell_buy_ratio_manager import SellBuyRatioManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager
from strategy.tools.indicator_provider.extensions.indicator_manager.vma_manager import VMAManager
from strategy.tools.kbar_indicators.kbar_indicator_center import KbarIndicatorCenter
from tools.utils import get_twse_date
from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.sd_stop_loss import SdStopLoss
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.indicator_manager.sell_buy_ratio_manager import SellBuyRatioManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager
from strategy.tools.indicator_provider.extensions.indicator_manager.vma_manager import VMAManager
from strategy.tools.kbar_indicators.kbar_indicator_center import KbarIndicatorCenter
from tools.utils import get_twse_date

@deprecated
class SDStopLossManager(AbsIndicatorManager):
    """

    """

    def __init__(self, rtm, symbol, start_time, redis: Redis,
                 sd_manager: StandardDeviationManager,
                 sd_length,
                 vma_manager: VMAManager,
                 kbar_indicator_center: KbarIndicatorCenter,
                 iiva_length,
                 iiva_interval,
                 sell_buy_ratio_manager,
                 sell_buy_ratio_change_rate_length,
                 pma_manager: PMAManager
                 ):
        super().__init__(IndicatorType.SD_STOP_LOSS, sd_length, symbol, start_time, redis, rtm)

        self.sd_manager = sd_manager
        self.vma_manager = vma_manager
        self.kbar_indicator_center = kbar_indicator_center
        self.iiva_length = iiva_length
        self.iiva_interval = iiva_interval
        self.sell_buy_ratio_manager: SellBuyRatioManager = sell_buy_ratio_manager
        self.sell_buy_ratio_change_rate_length = sell_buy_ratio_change_rate_length
        self.pma_manager = pma_manager

        self.now = None

    # @property
    # def _sensitivity(self):
    #     sd = float(self.sd_manager.get())  # 單位為點數
    #
    #     # 基準參數
    #     b = 3.5  # 基準乘數
    #     lo = 1.5 # 最低乘數（波動高時）
    #     hi = 6.0  # 最高乘數（波動低時）
    #
    #     # 設定點數門檻
    #     sd_l = 15  # 波動低的界線（震盪盤）
    #     sd_h = 25  # 波動高的界線（趨勢盤）
    #
    #     # 計算比例
    #     if sd <= sd_l:
    #         # 波動越低 → 乘數越高（最多到 hi）
    #         scale = (sd_l - sd) / sd_l
    #         return min(hi, b + scale * (hi - b))
    #     elif sd >= sd_h:
    #         # 波動越高 → 乘數越低（最少到 lo）
    #         scale = (sd - sd_h) / (100 - sd_h)  # 預設 100 是極端值上限
    #         return max(lo, b - scale * (b - lo))
    #     else:
    #         # 中間區間保持基準值
    #         return b

    # @property
    # def _sensitivity(self):
    #     sd = float(self.sd_manager.get())  # 單位為點數
    #
    #     # 基準參數
    #     b = 3.5  # 基準乘數
    #     lo = 1.5  # 最低乘數（波動高時）
    #     hi = 6.0  # 最高乘數（波動低時）
    #
    #     # 設定點數門檻
    #     sd_l = 15  # 波動低的界線（震盪盤）
    #     sd_h = 25  # 波動高的界線（趨勢盤）
    #
    #     # 計算比例
    #     if sd <= sd_l:
    #         # 波動越低 → 乘數越高（最多到 hi）
    #         scale = (sd_l - sd) / sd_l
    #         return max(lo, b - scale * (b - lo))
    #     elif sd >= sd_h:
    #         # 波動越高 → 乘數越低（最少到 lo）
    #         scale = (sd - sd_h) / (100 - sd_h)  # 預設 100 是極端值上限
    #         return min(hi, b + scale * (hi - b))
    #     else:
    #         # 中間區間保持基準值
    #         return b

    # @property
    # def _sensitivity(self):
    #     v = self.vma_manager.get()
    #     iiva = self.kbar_indicator_center.iiva.get(self.now, self.iiva_length, self.iiva_interval)
    #     return 3.5 - min((v / iiva) * 2, 2)

    @property
    def _n_loss(self):
        # return self.sd_manager.get() * self._sensitivity

        return self.sd_manager.get() * 1.618  # 1.5 for 震盪

    @staticmethod
    def _calc_n_loss_detail(sd: float, k: float = 1.5,
                            min_stop: float = 10, mid_start: float = 25, mid_end: float = 50,
                            max_stop: float = 80) -> float:
        """
        用標準差計算動態止損距離 (n_loss)
        - 小波動時跟得緊 (>= min_stop)
        - 中等波動時平滑壓縮
        - 大波動時乘係數並加上限 (max_stop)
        """
        raw_n_loss = sd * k

        # Case 1: 很小的波動 → 不小於 min_stop
        if raw_n_loss <= mid_start:
            return max(raw_n_loss, min_stop)

        # Case 2: 中等波動 → 線性壓縮
        elif raw_n_loss <= mid_end:
            # 從 mid_start ~ mid_end 之間，逐漸從 mid_start → mid_start + (mid_end - mid_start)*0.6
            ratio = (raw_n_loss - mid_start) / (mid_end - mid_start)
            compressed = mid_start + ratio * (mid_end - mid_start) * 0.6
            return compressed

        # Case 3: 大波動 → 放寬但不超過 max_stop
        else:
            return min(raw_n_loss * 0.7, max_stop)

    def _calc_n_loss(self, prev_val, price):
        sd = self.sd_manager.get()
        n_loss = sd * 3.5
        return min(80, n_loss)

    def _calc_n_loss_v2(self, prev_val, price):
        sd = self.sd_manager.get()

        adj = 0
        if sd > 50:
            adj = min(80, (sd - 50) * 0.5)
        return 25 + adj

    def _calc_n_loss_v3(self, prev_val, price):
        sd = self.sd_manager.get()
        n_loss = sd * 3.5
        n_loss_adj = 20 * math.log1p(n_loss / 10)
        return n_loss_adj

    # @staticmethod
    # def _calc_value_org(price, prev, prev_price, n_loss, prev_dir):
    #     if price > prev:
    #         iff1 = max(price - n_loss, price - prev_n_loss)
    #     elif price < prev:
    #         iff1 = min(price + n_loss, price + prev_n_loss)
    #     else:
    #         iff1 = prev
    #     iff2 = min(prev, price + n_loss) if price < prev and prev_price < prev else iff1
    #     value = max(prev, price - n_loss) if price > prev and prev_price > prev else iff2

    # @staticmethod
    # def _calc_value(price, prev, prev_price, n_loss, prev_dir):
    #     direction = 0
    #     value = prev
    #
    #     if price == prev:
    #         value = prev
    #         if prev_price > prev:
    #             direction = 1
    #         elif prev_price < prev:
    #             direction = -1
    #         else:
    #             direction = prev_dir
    #     elif prev_price == prev:
    #         if price > prev:
    #             direction = 1
    #             if prev_dir == 1:
    #                 value = prev
    #             elif prev_dir == -1:
    #                 value = price - n_loss
    #             else:
    #                 raise Exception('prev_dir should not be 0!')
    #         elif price < prev:
    #             direction = -1
    #             if prev_dir == -1:
    #                 value = prev
    #             elif prev_dir == 1:
    #                 value = price + n_loss
    #             else:
    #                 raise Exception('prev_dir should not be 0!')
    #         else:
    #             raise Exception('should not be here!')
    #
    #     elif prev_price > prev:
    #         if price > prev:
    #             value = max(prev, price - n_loss)
    #             direction = 1
    #         elif price < prev:
    #             value = price + n_loss
    #             direction = -1
    #         else:
    #             raise Exception('should not be here!')
    #     elif prev_price < prev:
    #         if price < prev:
    #             value = min(prev, price + n_loss)
    #             direction = -1
    #         elif price > prev:
    #             value = price - n_loss
    #             direction = 1
    #         else:
    #             raise Exception('should not be here!')
    #     else:
    #         raise Exception('should not be here!')
    #
    #     return value, direction

    @staticmethod
    def _calc_value(price, prev, prev_price, n_loss, prev_dir):
        def get_trend(a, b):
            if a > b: return 1
            if a < b: return -1
            return 0

        rel_prev = get_trend(prev_price, prev)
        rel_cur = get_trend(price, prev)

        # helper for the prev_price == prev cases that depend on prev_dir
        def equal_higher():
            if prev_dir == 1:
                return prev, 1
            if prev_dir == -1:
                return price - n_loss, 1
            raise Exception('prev_dir should not be 0!')

        def equal_lower():
            if prev_dir == -1:
                return prev, -1
            if prev_dir == 1:
                return price + n_loss, -1
            raise Exception('prev_dir should not be 0!')

        transitions = {
            # price == prev
            (0, 0): lambda: (prev, prev_dir),
            (1, 0): lambda: (prev, 1),
            (-1, 0): lambda: (prev, -1),

            # prev_price == prev (depends on prev_dir)
            (0, 1): equal_higher,
            (0, -1): equal_lower,

            # trend continuation
            (1, 1): lambda: (max(prev, price - n_loss), 1),
            (-1, -1): lambda: (min(prev, price + n_loss), -1),

            # trend reversal
            (1, -1): lambda: (price + n_loss, -1),
            (-1, 1): lambda: (price - n_loss, 1),
        }

        try:
            return transitions[(rel_prev, rel_cur)]()
        except KeyError:
            raise Exception(f"Unexpected state: prev_trend={rel_prev}, curr_trend={rel_cur}")

    def calculate(self, now, last: SdStopLoss):
        new = SdStopLoss()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        prev = last.value if last else 0

        price = self.pma_manager.get()
        prev_price = self.pma_manager.get(-2) or price

        twse_date = get_twse_date(now)
        self.now = now.replace(year=twse_date.year, month=twse_date.month, day=twse_date.day)

        n_loss = self._calc_n_loss(prev, price)
        prev_n_loss = last.n_loss if last else n_loss
        prev_dir = last.direction if last else 0

        value, direction = self._calc_value(price, prev, prev_price, n_loss, prev_dir)
        new.value = value
        new.direction = direction
        new.n_loss = n_loss

        return new

    # def calculate_v0(self, now, last: SdStopLoss):
    #     new = SdStopLoss()
    #     new.datetime = self.rtm.latest_tick().datetime
    #     new.indicator_type = self.indicator_type
    #     new.length = self.length
    #
    #     prev = last.value if last else 0
    #
    #     prev_tick = self.rtm.prev_tick()
    #
    #     price = self.rtm.latest_tick().close
    #     prev_price = prev_tick.close if prev_tick else price
    #
    #     twse_date = get_twse_date(now)
    #     self.now = now.replace(year=twse_date.year, month=twse_date.month, day=twse_date.day)
    #
    #     n_loss = self._calc_n_loss(prev, price)
    #     prev_n_loss = last.n_loss if last else n_loss
    #     prev_dir = last.direction if last else 0
    #
    #     value, direction = self._calc_value(price, prev, prev_price, n_loss, prev_dir)
    #     new.value = value
    #     new.direction = direction
    #     new.n_loss = n_loss
    #
    #     return new

    # def calculate_v2(self, now, last: SdStopLoss):
    #     new = SdStopLoss()
    #     new.datetime = self.rtm.latest_tick().datetime
    #     new.indicator_type = self.indicator_type
    #     new.length = self.length
    #
    #     prev = last.value if last else 0
    #
    #     if last:
    #         added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)
    #         price = sum(t.close * t.volume for t in added_ticks)/sum(t.volume for t in added_ticks)
    #
    #     else:
    #         price = self.rtm.latest_tick().close
    #
    #     prev_price = self._last_price if self._last_price else price
    #
    #     twse_date = get_twse_date(now)
    #     self.now = now.replace(year=twse_date.year, month=twse_date.month, day=twse_date.day)
    #
    #     n_loss = self._calc_n_loss(prev, price)
    #     prev_n_loss = last.n_loss if last else n_loss
    #     prev_dir = last.direction if last else 0
    #
    #     value, direction = self._calc_value(price, prev, prev_price, n_loss, prev_dir)
    #     new.value = value
    #     new.direction = direction
    #     new.n_loss = n_loss
    #
    #     self._last_price = price
    #
    #     return new
