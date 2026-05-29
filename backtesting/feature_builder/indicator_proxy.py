from datetime import timedelta

import numpy as np


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