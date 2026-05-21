"""
Feature Builder - ML 特徵計算模組

從 tick 資料計算 ML 模型所需的特徵矩陣。

命名規範（2026-03-30 確認）：
- tick_type == 1：外盤成交 → 買方主動 → 偏多
- tick_type == 2：內盤成交 → 賣方主動 → 偏空
- net_buy_ratio = (active_buy_vol - active_sell_vol) / total_vol
- active_buy_vol = 外盤成交量（tick_type == 1）
- active_sell_vol = 內盤成交量（tick_type == 2）
"""

import numpy as np
import pandas as pd
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional

from database.schema.history_tick import HistoryTick
from qclaw.backtesting.npy_cached_history_tick_manager import TickSlice


@dataclass
class FeatureConfig:
    """特徵計算設定"""
    # 時間窗口設定
    net_buy_window_s: timedelta = timedelta(minutes=15)  # 短
    net_buy_window_m: timedelta = timedelta(minutes=30)  # 中
    net_buy_window_l: timedelta = timedelta(minutes=60)  # 長
    sd_window: timedelta = timedelta(minutes=45)  # 標準差窗口
    momentum_window_short: timedelta = timedelta(minutes=5)  # 短動量
    momentum_window_long: timedelta = timedelta(minutes=15)  # 長動量

    # IIVA 設定
    iiva_length: timedelta = timedelta(days=5)
    iiva_interval_minutes: int = 5

    # volume_ratio 窗口
    volume_ratio_window: timedelta = timedelta(minutes=5)

    # Donchian 累計設定
    donchian_window: timedelta = timedelta(minutes=30)


class FeatureBuilder:
    """
    從 tick 資料計算 ML 特徵。

    產出特徵：
    - net_buy_ratio_s/m/l：不同窗口的淨買比率
    - volume_ratio：量比（rolling_vol_sum / iiva）
    - sd：45分鐘標準差
    - momentum_short：5分鐘價格動量
    - momentum_long：15分鐘價格動量
    - bid_ask_diff：買賣價差
    - bid_ask_imbalance：買賣量差

    使用方式：
    ```python
    from tools.batch_backtester import DayIndicators

    # 方法1：直接從 DayIndicators 建立（已計算好的指標）
    fb = FeatureBuilder.from_day_indicators(day_indicators)

    # 方法2：從 raw tick 資料建立
    fb = FeatureBuilder(ticks, iiva_lookup)

    # 取得特徵矩陣
    features = fb.build()
    X = features.to_numpy()
    ```
    """

    FEATURE_NAMES = [
        'net_buy_ratio_s',
        'net_buy_ratio_m',
        'net_buy_ratio_l',
        'volume_ratio',
        'sd',
        'momentum_short',
        'momentum_long',
        'bid_ask_diff',
        'bid_ask_imbalance',
        'price',
        'hour_minute',
        'net_buy_ratio_change',
        'net_buy_ratio_regime',
        'donchian_ha',
        'donchian_la',
        'donchian_dir',
        'donchian_h',
        'donchian_l',
    ]

    def __init__(
            self,
            ticks: list | TickSlice,
            iiva_lookup: Optional[Callable[[datetime], float]] = None,
            config: Optional[FeatureConfig] = None,
    ):
        """
        :param ticks: list[HistoryTick] 或 TickSlice
        :param iiva_lookup: 查詢 IIVA 的函式，若不傳入 volume_ratio 會設為 1.0
        :param config: 特徵計算設定
        """
        self._ticks = ticks
        self._config = config or FeatureConfig()
        self._iiva_lookup = iiva_lookup
        self._n = len(ticks)

        if not ticks:
            raise Exception('no ticks given.')
        if isinstance(ticks, TickSlice):
            self._init_attrs_by_tick_slice(ticks)
        elif isinstance(ticks[0], HistoryTick):
            self._init_attrs_by_history_ticks(ticks)
        else:
            raise Exception('unsupported tick type.')

        # 快取計算結果
        self._cache: dict = {}

    def _init_attrs_by_tick_slice(self, ticks: TickSlice):
        # 解析 tick 資料
        self._times = ticks.ts
        self._closes = ticks.close
        self._volumes = ticks.volume
        self._tick_types = ticks.tick_type
        self._bid_prices = ticks.bid_price
        self._ask_prices = ticks.ask_price
        self._bid_volumes = ticks.bid_volume
        self._ask_volumes = ticks.ask_volume

    def _init_attrs_by_history_ticks(self, ticks: list[HistoryTick]):
        self._times = np.array([t.ts.timestamp() for t in ticks], dtype=np.float64)
        self._closes = np.array([float(t.close) for t in ticks], dtype=np.float64)
        self._volumes = np.array([t.volume for t in ticks], dtype=np.int64)
        self._tick_types = np.array([t.tick_type for t in ticks], dtype=np.int32)
        self._bid_prices = np.array([float(t.bid_price) if t.bid_price else 0.0 for t in ticks], dtype=np.float64)
        self._ask_prices = np.array([float(t.ask_price) if t.ask_price else 0.0 for t in ticks], dtype=np.float64)
        self._bid_volumes = np.array([t.bid_volume if t.bid_volume else 0 for t in ticks], dtype=np.int64)
        self._ask_volumes = np.array([t.ask_volume if t.ask_volume else 0 for t in ticks], dtype=np.int64)

    @classmethod
    def from_day_indicators(cls, indicators, config: Optional[FeatureConfig] = None):
        """
        從 DayIndicators 物件快速建立 FeatureBuilder。

        :param indicators: DayIndicators 實例
        :param config: 特徵計算設定
        """

        # 建立一個 proxy 物件，委託給 DayIndicators 的方法
        class IndicatorsProxy:
            def __init__(self, ind):
                self._ind = ind
                self._n = ind.n
                self._times = ind.times
                self._closes = ind.closes
                self._volumes = ind.volumes
                self._tick_types = ind.tick_types

            @property
            def n(self):
                return self._n

            def _net_buy_ratio(self, window: timedelta) -> np.ndarray:
                """計算 net_buy_ratio，委託給內部方法"""
                return self._compute_net_buy_ratio(window)

            def _compute_net_buy_ratio(self, window: timedelta) -> np.ndarray:
                seconds = window.total_seconds()
                # tick_type == 1: 外盤 = 買方主動
                # tick_type == 2: 內盤 = 賣方主動
                active_buy_vol = np.where(self._tick_types == 1, self._volumes, 0)
                active_sell_vol = np.where(self._tick_types == 2, self._volumes, 0)

                active_buy_cum = np.zeros(self._n + 1, dtype=np.int64)
                active_sell_cum = np.zeros(self._n + 1, dtype=np.int64)
                np.cumsum(active_buy_vol, out=active_buy_cum[1:])
                np.cumsum(active_sell_vol, out=active_sell_cum[1:])

                result = np.full(self._n, 0.0, dtype=np.float64)
                times = self._times

                for i in range(self._n):
                    hi = times[i]
                    lo = hi - seconds
                    left = int(np.searchsorted(times, lo, side='right'))
                    right = i + 1
                    bv = active_buy_cum[right] - active_buy_cum[left]
                    sv = active_sell_cum[right] - active_sell_cum[left]
                    total = bv + sv
                    result[i] = (bv - sv) / total if total > 0 else 0.0

                return result

        proxy = IndicatorsProxy(indicators)
        fb = cls.__new__(cls)
        fb._ticks = []
        fb._config = config or FeatureConfig()
        fb._iiva_lookup = None
        fb._n = indicators.n
        fb._times = indicators.times
        fb._closes = indicators.closes
        fb._volumes = indicators.volumes
        fb._tick_types = indicators.tick_types
        fb._bid_prices = np.zeros(indicators.n, dtype=np.float64)
        fb._ask_prices = np.zeros(indicators.n, dtype=np.float64)
        fb._bid_volumes = np.zeros(indicators.n, dtype=np.int64)
        fb._ask_volumes = np.zeros(indicators.n, dtype=np.int64)
        fb._cache = {}

        # 將 indicators 的屬性委託給 proxy
        fb._indicators_proxy = proxy
        return fb

    # ─────────────────────────────────────────────
    #  核心計算方法
    # ─────────────────────────────────────────────

    def build(self, feature_names: list[str] = None) -> pd.DataFrame:
        """
        計算所有特徵，回傳 DataFrame。

        :return: 特徵矩陣，每列是一個 tick，每行是一個特徵
        """

        func_map = {
            'price': lambda: self._closes,
            'net_buy_ratio_s': self.net_buy_ratio_s,
            'net_buy_ratio_m': self.net_buy_ratio_m,
            'net_buy_ratio_l': self.net_buy_ratio_l,
            'volume_ratio': self.volume_ratio,
            'sd': self.sd,
            'momentum_short': self.momentum_short,
            'momentum_long': self.momentum_long,
            'bid_ask_diff': self.bid_ask_diff,
            'bid_ask_imbalance': self.bid_ask_imbalance,
            'hour_minute': self._hour_minute,
            'net_buy_ratio_change': self.net_buy_ratio_change,
            'net_buy_ratio_regime': self.net_buy_ratio_regime,
            'donchian_ha': self.donchian_ha,
            'donchian_la': self.donchian_la,
            'donchian_dir': self.donchian_dir,
            'donchian_h': self.donchian_h,
            'donchian_l': self.donchian_l,
        }

        if not feature_names:
            data = {k: v() for k, v in func_map.items()}

        else:
            data = {}
            for n in feature_names:
                if n not in func_map:
                    raise Exception("Unknown feature: {}".format(n))
                data[n] = func_map[n]()

        return pd.DataFrame(data, index=self._times)

    def build_with_labels(self, labels: np.ndarray) -> pd.DataFrame:
        """
        計算特徵並附加標籤。

        :param labels: 標籤陣列（與 tick 數量相同）
        :return: 含標籤的 DataFrame
        """
        df = self.build()
        df['label'] = labels
        return df

    # ─────────────────────────────────────────────
    #  個別特徵計算
    # ─────────────────────────────────────────────

    def net_buy_ratio_s(self) -> np.ndarray:
        """1分鐘淨買比率"""
        return self._net_buy_ratio(self._config.net_buy_window_s)

    def net_buy_ratio_m(self) -> np.ndarray:
        """5分鐘淨買比率"""
        return self._net_buy_ratio(self._config.net_buy_window_m)

    def net_buy_ratio_l(self) -> np.ndarray:
        """10分鐘淨買比率"""
        return self._net_buy_ratio(self._config.net_buy_window_l)

    def _net_buy_ratio(self, window: timedelta) -> np.ndarray:
        """
        計算滾動窗口內的淨買比率。

        net_buy_ratio = (active_buy_vol - active_sell_vol) / total_vol
        > 0: 買方主動（外盤多）
        < 0: 賣方主動（內盤多）
        """
        cache_key = f'net_buy_ratio_{window}'
        if cache_key in self._cache:
            return self._cache[cache_key]

        seconds = window.total_seconds()

        # tick_type == 1: 外盤 = 買方主動
        # tick_type == 2: 內盤 = 賣方主動
        active_buy_vol = np.where(self._tick_types == 1, self._volumes, 0)
        active_sell_vol = np.where(self._tick_types == 2, self._volumes, 0)

        active_buy_cum = np.zeros(self._n + 1, dtype=np.int64)
        active_sell_cum = np.zeros(self._n + 1, dtype=np.int64)
        np.cumsum(active_buy_vol, out=active_buy_cum[1:])
        np.cumsum(active_sell_vol, out=active_sell_cum[1:])

        result = np.full(self._n, 0.0, dtype=np.float64)

        for i in range(self._n):
            hi = self._times[i]
            lo = hi - seconds
            left = int(np.searchsorted(self._times, lo, side='right'))
            right = i + 1

            bv = active_buy_cum[right] - active_buy_cum[left]
            sv = active_sell_cum[right] - active_sell_cum[left]
            total = bv + sv
            result[i] = (bv - sv) / total if total > 0 else 0.0

        self._cache[cache_key] = result
        return result

    def net_buy_ratio_change(self) -> np.ndarray:
        """
        淨買比率動量變化（中期 - 長期）
        捕捉買賣力道從短期到中期的轉變
        """
        nbr_m = self.net_buy_ratio_m()
        nbr_l = self.net_buy_ratio_l()
        return nbr_m - nbr_l

    def net_buy_ratio_regime(self) -> np.ndarray:
        """
        買盤排列強度：l > m > s 或 l < m < s 時才啟動
        只有三條均線呈現明確排列方向時才計算 (l - s)，否則為 0。

        l > m > s → 正值（買盤越來越強）
        l < m < s → 負值（買盤越來越弱）
        其他     → 0（排列不明確）
        """
        nbr_s = self.net_buy_ratio_s()
        nbr_m = self.net_buy_ratio_m()
        nbr_l = self.net_buy_ratio_l()

        long_arr = (nbr_l > nbr_m) & (nbr_m > nbr_s)
        short_arr = (nbr_l < nbr_m) & (nbr_m < nbr_s)
        aligned = long_arr | short_arr

        result = np.zeros_like(nbr_s)
        result[aligned] = nbr_l[aligned] - nbr_s[aligned]
        return result

    def volume_ratio(self) -> np.ndarray:
        """
        量比 = 滾動窗口內成交量總和 / IIVA（歷史同期均量）

        若無 iiva_lookup，回傳 1.0
        """
        cache_key = 'volume_ratio'
        if cache_key in self._cache:
            return self._cache[cache_key]

        # rolling_vol_sum：過去5分鐘內所有tick的成交量總和
        rolling_vol_sum = self._rolling_vol_sum_window(
            self._config.volume_ratio_window
        )

        if self._iiva_lookup is None:
            self._cache[cache_key] = np.ones(self._n, dtype=np.float64)
            return self._cache[cache_key]

        # 建立 IIVA 查詢表
        iiva_array = np.full(self._n, 1.0, dtype=np.float64)
        for i in range(self._n):
            ts = datetime.fromtimestamp(self._times[i])
            aligned = ts.replace(second=0, microsecond=0)
            iiva_array[i] = self._iiva_lookup(aligned)

        result = np.where(
            iiva_array > 0,
            rolling_vol_sum / iiva_array,
            1.0
        )

        self._cache[cache_key] = result
        return result

    def _rolling_vol_sum_window(self, window: timedelta) -> np.ndarray:
        """滾動窗口內的成交量總和（O(n) 累積和技巧）"""
        seconds = window.total_seconds()
        result = np.full(self._n, 0.0, dtype=np.float64)
        cum_vol = np.zeros(self._n + 1, dtype=np.int64)
        np.cumsum(self._volumes, out=cum_vol[1:])

        for i in range(self._n):
            hi = self._times[i]
            lo = hi - seconds
            left = int(np.searchsorted(self._times, lo, side='right'))
            right = i + 1
            result[i] = cum_vol[right] - cum_vol[left]

        return result

    def sd(self) -> np.ndarray:
        """45分鐘滾動標準差"""
        cache_key = 'sd'
        if cache_key in self._cache:
            return self._cache[cache_key]

        window = self._config.sd_window
        seconds = window.total_seconds()
        result = np.full(self._n, 0.0, dtype=np.float64)

        cum_c = np.zeros(self._n + 1, dtype=np.float64)
        cum_c2 = np.zeros(self._n + 1, dtype=np.float64)
        np.cumsum(self._closes, out=cum_c[1:])
        np.cumsum(self._closes ** 2, out=cum_c2[1:])

        for i in range(self._n):
            lo = self._times[i] - seconds
            left = int(np.searchsorted(self._times, lo, side='right'))
            right = i + 1
            n = right - left
            if n <= 1:
                result[i] = 0.0
                continue

            mean = (cum_c[right] - cum_c[left]) / n
            mean2 = (cum_c2[right] - cum_c2[left]) / n
            var = mean2 - mean * mean
            result[i] = np.sqrt(var) if var > 0 else 0.0

        self._cache[cache_key] = result
        return result

    def momentum_short(self) -> np.ndarray:
        """5分鐘價格動量（變化率）"""
        return self._momentum(self._config.momentum_window_short)

    def momentum_long(self) -> np.ndarray:
        """15分鐘價格動量（變化率）"""
        return self._momentum(self._config.momentum_window_long)

    def _momentum(self, window: timedelta) -> np.ndarray:
        """
        計算價格動量（相對變化率）。

        momentum = (current_price - price_N_minutes_ago) / price_N_minutes_ago
        """
        seconds = window.total_seconds()
        result = np.full(self._n, 0.0, dtype=np.float64)

        for i in range(self._n):
            hi = self._times[i]
            lo = hi - seconds
            left = int(np.searchsorted(self._times, lo, side='right'))

            if left < i:
                past_price = self._closes[left]
                if past_price > 0:
                    result[i] = (self._closes[i] - past_price) / past_price
                else:
                    result[i] = 0.0

        return result

    def bid_ask_diff(self) -> np.ndarray:
        """買賣價差（ask_price - bid_price）"""
        return self._ask_prices - self._bid_prices

    def bid_ask_imbalance(self) -> np.ndarray:
        """
        買賣量差比率 = (bid_volume - ask_volume) / (bid_volume + ask_volume)
        > 0: 買方掛單多
        < 0: 賣方掛單多
        """
        total = self._bid_volumes + self._ask_volumes
        with np.errstate(invalid='ignore', divide='ignore'):
            result = np.where(
                total > 0,
                (self._bid_volumes - self._ask_volumes) / total,
                0.0
            )
        return result

    def _hour_minute(self) -> np.ndarray:
        """時間特徵（轉換為分鐘數：hour * 60 + minute）"""
        result = np.zeros(self._n, dtype=np.float64)
        for i in range(self._n):
            dt = datetime.fromtimestamp(self._times[i])
            # 轉換為台灣時區（已在校驗資料中處理）
            result[i] = dt.hour * 60 + dt.minute
        return result

    # ─────────────────────────────────────────────
    #  多時間框架特徵（新增）
    # ─────────────────────────────────────────────

    # ─────────────────────────────────────────────
    #  Donchian 累計特徵
    # ─────────────────────────────────────────────

    def donchian_ha(self) -> np.ndarray:
        """唐奇安往上累計次數（ha）"""
        ha, _, _, _, _ = self._donchian_accumulation()
        return ha

    def donchian_la(self) -> np.ndarray:
        """唐奇安往下累計次數（la）"""
        _, la, _, _, _ = self._donchian_accumulation()
        return la

    def donchian_dir(self) -> np.ndarray:
        """唐奇安方向（1=往上, -1=往下, 0=中性）"""
        _, _, dir_arr, _, _ = self._donchian_accumulation()
        return dir_arr

    def donchian_h(self) -> np.ndarray:
        """唐奇安往上累計次數（ha）"""
        _, _, _, h, _ = self._donchian_accumulation()
        return h

    def donchian_l(self) -> np.ndarray:
        """唐奇安往下累計次數（la）"""
        _, _, _, _, l = self._donchian_accumulation()
        return l

    def _donchian_accumulation(self) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        單調雙端佇列實作滾動 Donchian Channel + ha/la 累計。

        規則：
          price > h 且 la == 0 → ha += 1, h = price
          price > h 且 la != 0 → ha = 1, la = 0, h = price
          price < l 且 ha == 0 → la += 1, l = price
          price < l 且 ha != 0 → la = 1, ha = 0, l = price

        視窗過期時，h/l 由單調 deque 動態維護。
        時間複雜度：O(n)，每個元素最多進 deque 一次。
        """
        cache_key = 'donchian_accumulation'
        if cache_key in self._cache:
            return self._cache[cache_key]

        n = self._n
        seconds = self._config.donchian_window.total_seconds()
        prices = self._closes
        times = self._times

        h_arr = np.zeros(n, dtype=np.float64)
        l_arr = np.zeros(n, dtype=np.float64)
        ha_arr = np.zeros(n, dtype=np.float64)
        la_arr = np.zeros(n, dtype=np.float64)
        dir_arr = np.zeros(n, dtype=np.float64)

        # 單調 deque：(ts, price)
        h_q: deque = deque()  # 遞減，前端 = 視窗 max
        l_q: deque = deque()  # 遞增，前端 = 視窗 min

        ha = la = 0
        direction = 0  # 1=up, -1=down, 0=neutral

        for i in range(n):
            ts = times[i]
            price = prices[i]
            window_left = ts - seconds

            # 1. 移除過期元素（從前端）
            while h_q and h_q[0][0] < window_left:
                h_q.popleft()
            while l_q and l_q[0][0] < window_left:
                l_q.popleft()

            # 2. 取出視窗 max / min（插入新 tick 之前的值）
            if h_q:
                h = h_q[0][1]
            else:
                h = price  # deque 為空，設為當前價格

            if l_q:
                l = l_q[0][1]
            else:
                l = price  # deque 為空，設為當前價格

            # 3. 套用 4 條規則更新 ha / la / dir（比較的是舊的 h/l）
            if price > h:
                if la == 0:
                    ha += 1
                else:
                    ha = 1
                    la = 0
                direction = 1
            elif price < l:
                if ha == 0:
                    la += 1
                else:
                    la = 1
                    ha = 0
                direction = -1
            # else: 區間內，不變

            # 4. 維護單調性 + 插入新元素
            # h_q（遞減）：新値比後端大就 pop 後端
            while h_q and h_q[-1][1] <= price:
                h_q.pop()
            h_q.append((ts, price))

            # l_q（遞增）：新値比後端小就 pop 後端
            while l_q and l_q[-1][1] >= price:
                l_q.pop()
            l_q.append((ts, price))

            h_arr[i] = h
            l_arr[i] = l
            ha_arr[i] = ha
            la_arr[i] = la
            dir_arr[i] = direction

        self._cache[cache_key] = (ha_arr, la_arr, dir_arr, h_arr, l_arr)
        return ha_arr, la_arr, dir_arr, h_arr, l_arr

    # ─────────────────────────────────────────────
    #  工具方法
    # ─────────────────────────────────────────────

    @property
    def n(self) -> int:
        """回傳 tick 總數"""
        return self._n

    @property
    def times(self) -> np.ndarray:
        """回傳時間戳陣列"""
        return self._times

    @property
    def closes(self) -> np.ndarray:
        """回傳收盤價陣列"""
        return self._closes

    def get_feature_names(self) -> list[str]:
        """回傳特徵名稱列表"""
        return self.FEATURE_NAMES.copy()

    def summary(self) -> dict:
        """回傳特徵統計摘要"""
        features = self.build()
        return {
            'n_samples': len(features),
            'feature_stats': features.describe().to_dict(),
        }
