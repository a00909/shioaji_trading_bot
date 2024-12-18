import datetime
from functools import lru_cache

import numpy as np
import pandas as pd
from line_profiler_pycharm import profile
from sklearn.linear_model import LinearRegression

from tick_manager.realtime_tick_manager import RealtimeTickManager
from tools.cache_manager import CacheManager
from tools.utils import get_now

cache_manager = CacheManager()


class IndicatorProvider:

    def __init__(self, rtm):
        self.rtm: RealtimeTickManager = rtm

    def start(self):
        self.rtm.start()
        self.rtm.wait_for_ready()

    def stop(self):
        self.rtm.stop()
        print('ip stopped.')

    def latest_price(self):
        latest_price = self.rtm.get_ticks_by_backward_idx(0)[0].close
        return latest_price

    def wait_for_tick(self):
        return self.rtm.wait_for_tick()

    # def wait_for_ready(self):
    #     self.rtm.ready_event.wait()

    # @profile
    @lru_cache
    def ma(self, length: datetime.timedelta):
        ticks = self.rtm.get_ticks_by_backtracking_time(length)
        if len(ticks) == 0:
            return 0

        avg = sum([tick.close * tick.volume for tick in ticks]) / sum([tick.volume for tick in ticks])
        return avg

    # @profile
    @lru_cache
    def is_increasing(self, length: datetime.timedelta, window_size=3):
        ticks = self.rtm.get_ticks_by_backtracking_time(length)
        if len(ticks) == 0:
            return False, 0
        # if len(ticks) <= 1:
        #     return False, 0
        # 提取 close 值，使用 NumPy 陣列以提高效率
        close_values = np.array([float(tick.close) for tick in ticks])

        # # 計算簡單移動平均
        # def moving_average(values, window_size):
        #     return np.convolve(values, np.ones(window_size) / window_size, mode='valid')
        #
        # sma = moving_average(close_values, window_size)

        # 使用最小二乘法計算斜率
        x = np.arange(len(close_values))
        x_mean, y_mean = np.mean(x), np.mean(close_values)
        slope = np.sum((x - x_mean) * (close_values - y_mean)) / np.sum((x - x_mean) ** 2)

        return slope > 0, slope

    def is_increasing_bk(self, length: datetime.timedelta, window_size=3):
        ticks = self.rtm.get_ticks_by_backtracking_time(length)

        series = pd.Series([tick.close for tick in ticks])

        # 計算簡單移動平均
        sma = series.rolling(window=window_size).mean()

        # 準備數據進行線性回歸分析
        x = np.arange(len(series)).reshape(-1, 1)  # 特徵：索引
        y = series.values.reshape(-1, 1)  # 標籤：數值

        # 創建線性回歸模型並擬合數據
        model = LinearRegression()
        model.fit(x, y)

        # 獲取斜率
        slope = model.coef_[0][0]

        # 判斷趨勢
        if slope > 0:
            trend_status = "上升趨勢"
        else:
            trend_status = "無上升趨勢"

        return slope > 0, slope

    @lru_cache
    def vol_avg(self, length: datetime.timedelta, unit: datetime.timedelta, with_msg=False) -> tuple[
                                                                                                   float, str] | float:
        amounts = length.total_seconds() / unit.total_seconds()
        ticks = self.rtm.get_ticks_by_backtracking_time(length)
        if len(ticks) == 0:
            if with_msg:
                return 0, 'No data'
            else:
                return 0

        start = ticks[0].datetime
        end = ticks[-1].datetime
        avg = sum([tick.volume for tick in ticks]) / amounts

        if with_msg:
            msg = (
                f'[Vol avg info (l={length.total_seconds()} s)]\n'
                f'start: {start.strftime("%H:%M:%S")} | end: {end.strftime("%H:%M:%S")}\n'
                f'| avg: {avg} | per: {unit} | delta:{(end - start)}\n'
                f'| {len(ticks)} ticks'
            )
            return avg, msg

        return avg

    @lru_cache
    def atr(self, length: datetime.timedelta, unit: datetime.timedelta):
        """
        計算 ATR (Average True Range) 基於固定的時間單位
        :param length: ATR 計算的總時間長度
        :param unit: ATR 分段計算的時間單位
        :return: ATR 值
        """

        ticks = self.rtm.get_ticks_by_backtracking_time(length)
        if len(ticks) == 0:
            return 0

        end = get_now()
        start = end - length
        amounts = int(length.total_seconds() / unit.total_seconds())

        if not ticks:
            return -1

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

        atr = sum(trs) / len(trs)
        return atr

    def clear_lru_cache(self):
        self.atr.cache_clear()
        self.ma.cache_clear()
        self.is_increasing.cache_clear()
        self.vol_avg.cache_clear()
