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

from datetime import datetime, timedelta, date
from functools import wraps
from typing import Callable, Optional

import numpy as np

from backtesting.feature_builder._feature_calculators.bid_ask_imbalance import bid_ask_features, compute_imb_change_rate
from backtesting.feature_builder._feature_calculators.time_feature import extract_time_features_util
from backtesting.feature_builder._feature_calculators.volume_ratio import volume_ratio
from backtesting.feature_builder._feature_calculators.momentum import momentum
from backtesting.feature_builder._feature_calculators.sd import sd
from backtesting.feature_builder._feature_config import FeatureConfig
from backtesting.feature_builder._feature_calculators.donchian import donchian
from backtesting.feature_builder._feature_calculators.net_buy_ratio import net_buy_ratio
from backtesting.feature_builder.feature_name import FeatureName
from backtesting.feature_builder.indicator_proxy import IndicatorsProxy
from backtesting.feature_builder.labels.pnl_label import pnl_label
from backtesting.feature_builder.labels.triple_barrier_label import triple_barrier_label
from data_manager.history.statics.aggregated_bars import AggregatedBars
from data_manager.history.statics.tick.np_ticks import NPTicks
from database.schema.history_tick import HistoryTick
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.iiva_lookup import IIVALookup


def cache_result(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        cache_key = (
            func.__name__,
            args,
            tuple(sorted(kwargs.items()))
        )

        if cache_key in self._cache:
            return self._cache[cache_key]

        result = func(self, *args, **kwargs)

        self._cache[cache_key] = result
        return result

    return wrapper


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

    def __init__(
            self,
            ticks: list | NPTicks | AggregatedBars,
            iiva_lookups: dict[date,IIVALookup]=None,
            config: Optional[FeatureConfig] = None,
    ):
        """
        :param ticks: list[HistoryTick] 或 TickSlice
        :param iiva_lookup: 查詢 IIVA 的函式，若不傳入 volume_ratio 會設為 1.0
        :param config: 特徵計算設定
        """
        self._ticks = ticks
        self._config = config or FeatureConfig()
        self._iiva_lookups = iiva_lookups
        self._n = len(ticks)

        if not ticks:
            raise Exception('no ticks given.')
        if isinstance(ticks, NPTicks):
            self._init_attrs_by_tick_slice(ticks)
        elif isinstance(ticks, AggregatedBars):
            self._init_attrs_by_aggregated_bars(ticks)
        elif isinstance(ticks[0], HistoryTick):
            self._init_attrs_by_history_ticks(ticks)
        else:
            raise Exception('unsupported tick type.')

        # 快取計算結果
        self._cache: dict = {}

    def _init_attrs_by_tick_slice(self, ticks: NPTicks):
        # 解析 tick 資料
        self._times = ticks.ts_seconds()
        self._closes = ticks.close
        self._volumes = ticks.volume
        self._tick_types = ticks.tick_type
        self._bid_prices = ticks.bid_price
        self._ask_prices = ticks.ask_price
        self._bid_volumes = ticks.bid_volume
        self._ask_volumes = ticks.ask_volume

    def _init_attrs_by_aggregated_bars(self, bars: AggregatedBars):
        self._times = bars.ts_seconds()
        self._closes = bars.close
        self._volumes = bars.volume
        # 用成交方向重建 tick_type：外盤>內盤→偏多(1)，反之偏空(2)
        diff = bars.active_buy_volume - bars.active_sell_volume
        self._tick_types = np.where(diff >= 0, 1, 2).astype(np.int32)
        self._bid_prices = bars.avg_bid_price
        self._ask_prices = bars.avg_ask_price
        self._bid_volumes = bars.avg_bid_volume.astype(np.int64)
        self._ask_volumes = bars.avg_ask_volume.astype(np.int64)

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

    def build_features(self, feature_names: list[str] = None, return_mtx=False) -> dict | np.ndarray:
        """
        計算所有特徵，回傳 dict[str,ndarray]。

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
            'bid_ask_imbalance': self.bid_ask_features,
            'net_buy_ratio_change': self.net_buy_ratio_change,
            'net_buy_ratio_regime': self.net_buy_ratio_regime,
            'donchian_ha': self.donchian_ha,
            'donchian_la': self.donchian_la,
            'donchian_dir': self.donchian_dir,
            'donchian_h': self.donchian_h,
            'donchian_l': self.donchian_l,
            FeatureName.SIN_TIME: lambda: self._time()[FeatureName.SIN_TIME],
            FeatureName.COS_TIME: lambda: self._time()[FeatureName.COS_TIME],
            FeatureName.IS_OP_30: lambda: self._time()[FeatureName.IS_OP_30],
            FeatureName.IS_CL_30: lambda: self._time()[FeatureName.IS_CL_30],
            FeatureName.DIR_SD: self.directional_sd,
            FeatureName.DC_BRKOUT_ACCU: self.dc_breakout_accu,
            'dc_energy': self.dc_energy,
            'ba_imb': self.ba_imb,
            'ba_imb_cr': self.ba_imb_cr,
        }

        if not feature_names:
            data = {k: v() for k, v in func_map.items()}

        else:
            data: dict[str, np.ndarray] = {}
            for n in feature_names:
                if n not in func_map:
                    raise Exception("Unknown feature: {}".format(n))
                data[n] = func_map[n]()


        if return_mtx:
            mtx = np.hstack([data[k].reshape(-1, 1) for k in data])
            return mtx
        return data

    def build_label(self):
        label = self.tbl_label()
        valid = self.tbl_valid()
        return label, valid

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

    @cache_result
    def _net_buy_ratio(self, window: timedelta) -> np.ndarray:
        result = net_buy_ratio(self.times, self._tick_types, self._volumes, window.total_seconds())

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

    @cache_result
    def volume_ratio(self) -> np.ndarray:
        result = volume_ratio(
            self._n,
            self._config.volume_ratio_window.total_seconds(),
            self._times,
            self._volumes,
            self._iiva_lookups
        )
        return result

    @cache_result
    def sd(self) -> np.ndarray:
        result = sd(self._n, self._config.sd_window.total_seconds(), self._times, self._closes)
        return result

    def directional_sd(self):
        return self.donchian_dir() * self.sd()

    def momentum_short(self) -> np.ndarray:
        """5分鐘價格動量（變化率）"""
        return self._momentum(self._config.momentum_window_short)

    def momentum_long(self) -> np.ndarray:
        """15分鐘價格動量（變化率）"""
        return self._momentum(self._config.momentum_window_long)

    def _momentum(self, window: timedelta) -> np.ndarray:
        return momentum(self._n, window.total_seconds(), self._times, self._closes)

    def bid_ask_diff(self) -> np.ndarray:
        """買賣價差（ask_price - bid_price）"""
        return self._ask_prices - self._bid_prices

    @cache_result
    def bid_ask_features(self):
        imb_windowed, mid_windowed, spread_windowed = bid_ask_features(
            self._n,
            self._bid_volumes,
            self._ask_volumes,
            self._bid_prices,
            self._ask_prices,
            self._times,
            self._config.bid_ask_imb_window.total_seconds())
        return imb_windowed, mid_windowed, spread_windowed

    def ba_imb(self):
        imb, _, _ = self.bid_ask_features()
        return imb

    def ba_mid(self):
        _, mid, _ = self.bid_ask_features()
        return mid

    def ba_imb_cr(self):
        cr = compute_imb_change_rate(
            self._n,
            self._times,
            self.ba_imb()
        )
        return cr

    @cache_result
    def _time(self) -> dict[str, np.ndarray]:
        """時間特徵（轉換為分鐘數：hour * 60 + minute）"""
        tf = extract_time_features_util(self._times)
        return tf

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

    def dc_breakout_accu(self):
        return self.donchian_ha() - self.donchian_la()

    def dc_energy(self):
        return self.donchian_ha() + self.donchian_la()

    @cache_result
    def _donchian_accumulation(self, use_ba_mid=False) -> tuple[
        np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:

        if use_ba_mid:
            p = self.ba_imb()
        else:
            p = self._closes

        ha_arr, la_arr, dir_arr, h_arr, l_arr = donchian(
            self._n,
            self._config.donchian_window.total_seconds(),
            p,
            self._times
        )
        return ha_arr, la_arr, dir_arr, h_arr, l_arr

    # ─────────────────────────────────────────────
    #  label
    # ─────────────────────────────────────────────

    @cache_result
    def _pnl_label(self):
        future_max, future_min, upside, downside, valid_mask = pnl_label(
            self._closes, self._times, self._config.pnl_label_window.total_seconds())

        return future_max, future_min, upside, downside, valid_mask

    def max_fav(self):
        _, _, mf, _, _ = self._pnl_label()
        return mf

    def max_adv(self):
        _, _, _, ma, _ = self._pnl_label()
        return ma

    def pnl_valid_mask(self):
        _, _, _, _, valid_mask = self._pnl_label()
        return valid_mask

    @cache_result
    def _triple_barrier_label(self):
        label, profit, barrier_idx, valid_mask = triple_barrier_label(
            self._closes, self._times, self._config.pnl_label_window.total_seconds(),
            0.002, 0.0016
        )
        return label, profit, barrier_idx, valid_mask

    def tbl_label(self):
        l, _, _, _ = self._triple_barrier_label()
        return l

    def tbl_valid(self):
        _, _, _, v = self._triple_barrier_label()
        return v

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

    def summary(self) -> dict:
        """回傳特徵統計摘要"""
        features = self.build_features()
        return {
            'n_samples': len(features),
            'feature_stats': features.describe().to_dict(),
        }
