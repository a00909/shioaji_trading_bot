from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager
from strategy.tools.indicator_provider.extensions.indicator_manager.pma_manager import PMAManager
from strategy.tools.indicator_provider.extensions.indicator_manager.standard_deviation_manager import \
    StandardDeviationManager


class SDStopLossManager(AbsIndicatorManager):
    """

    """

    def __init__(self, sd_length, pma_length, rtm, symbol, start_time, redis: Redis,
                 sd_manager: StandardDeviationManager,
                 pma_manager: PMAManager):
        super().__init__(IndicatorType.SD_STOP_LOSS, sd_length, symbol, start_time, redis, rtm)

        self.end_square_sum = None
        self.end_count = None
        self.end_sum = None

        self.pma_length = pma_length
        self.pma_manager = pma_manager
        self.sd_manager = sd_manager

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

    @property
    def _sensitivity(self):
        sd = float(self.sd_manager.get())  # 單位為點數

        # 基準參數
        b = 3.5  # 基準乘數
        lo = 1.5  # 最低乘數（波動高時）
        hi = 6.0  # 最高乘數（波動低時）

        # 設定點數門檻
        sd_l = 15  # 波動低的界線（震盪盤）
        sd_h = 25  # 波動高的界線（趨勢盤）

        # 計算比例
        if sd <= sd_l:
            # 波動越低 → 乘數越高（最多到 hi）
            scale = (sd_l - sd) / sd_l
            return max(lo, b - scale * (b - lo))
        elif sd >= sd_h:
            # 波動越高 → 乘數越低（最少到 lo）
            scale = (sd - sd_h) / (100 - sd_h)  # 預設 100 是極端值上限
            return min(hi, b + scale * (hi - b))
        else:
            # 中間區間保持基準值
            return b

    @property
    def _n_loss(self):
        # return self.sd_manager.get() * self._sensitivity
        return self.sd_manager.get() ** 1.75

    def calculate(self, now, last: Indicator):
        new = Indicator()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        prev = last.value if last else 0
        prev_tick = self.rtm.prev_tick()

        price = self.rtm.latest_tick().close
        prev_price = prev_tick.close if prev_tick else price

        # pma version
        # pma = self.pma_manager.get()
        # prev_pma = self.pma_manager.get(-2)
        #
        # price = pma
        # prev_price = prev_pma if prev_pma else pma

        iff1 = price - self._n_loss if price > prev else price + self._n_loss
        iff2 = min(prev, price + self._n_loss) if price < prev and prev_price < prev else iff1
        value = max(prev, price - self._n_loss) if price > prev and prev_price > prev else iff2

        new.value = value

        return new
