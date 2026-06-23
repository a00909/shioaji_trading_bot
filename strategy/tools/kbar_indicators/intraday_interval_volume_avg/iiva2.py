from datetime import date, datetime, timedelta, time
from functools import cache

import numpy as np
from psycopg import Connection

from data_manager.history.kbm2 import NpyCachedKBarManager
from data_manager.history.statics.kbar.np_kbars import NPKBars
from qclaw.backtesting.npy_cache.npy_cache_manager import npy_cache_manager, CacheState
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.iiva_lookup import IIVALookup
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.utils import align_minute, minute_index
from tools.constants import DATE_FORMAT_REDIS, DATE_FORMAT_DB_AND_SJ

MINUTES_PER_DAY = 1440


class IntradayIntervalVolumeAvg2:
    """
    儲存格式:
    [date][aligned_minute]
    """

    def __init__(self, api):
        self.kbar_manager: NpyCachedKBarManager = NpyCachedKBarManager(api)

    @staticmethod
    def _npy_cache_key(symbol, end: date, length: timedelta, interval_min):
        return f'{symbol}.intraday_interval_volume_avg.i{interval_min}m.l{length.days}D.{end.strftime(DATE_FORMAT_DB_AND_SJ)}'

    @staticmethod
    def _accum(group_by_minute):
        DAY_START = 8 * 60 + 45  # 08:45
        DAY_END = 13 * 60 + 45  # 13:45
        NIGHT_START = 15 * 60  # 15:00
        NIGHT_END = 5 * 60  # 05:00 (跨午夜)

        # 日盤
        accu = 0
        for cur in range(DAY_START, DAY_END):
            if cur in group_by_minute:
                accu += group_by_minute[cur]
                group_by_minute[cur] = accu

        # 夜盤
        accu = 0
        for cur in list(range(NIGHT_START, 24 * 60)) + list(range(0, NIGHT_END)):
            if cur in group_by_minute:
                accu += group_by_minute[cur]
                group_by_minute[cur] = accu

    def _calc(self, kbars: NPKBars, interval_min, accumulate=False) -> dict[int, float]:
        group_by_minute: dict[int, list[float]] | dict[int, float] = {}

        n = len(kbars)
        for i in range(n):
            ts: datetime = kbars.datetime_py()[i]
            key = align_minute(ts, interval_min)
            group_by_minute.setdefault(key, []).append(kbars.volume[i])

        for k, v in group_by_minute.items():
            group_by_minute[k] = sum(v) / len(v)

        if accumulate:
            IntradayIntervalVolumeAvg2._accum(group_by_minute)

        return group_by_minute

    @staticmethod
    def _to_ndarray(data: dict[int, float], interval):
        n = (MINUTES_PER_DAY + interval - 1) // interval  # 總資料筆數: 一天的分鐘數去除以interval，沒整除的地方也要算一筆，所以取ceil
        np_data = np.empty(n, dtype=np.float64)
        for i in range(n):
            if 5 * i in data:
                v = data[5 * i]
            else:
                v = 0
            np_data[i] = v

        return np_data



    @staticmethod
    @cache
    def _arr_from_npy(key):
        return npy_cache_manager.get(key)

    @staticmethod
    @cache
    def _single_value_from_npy(key, index) -> dict[int, float]:
        val = IntradayIntervalVolumeAvg2._arr_from_npy(key)[index]  # 一天範圍的資料
        return val

    def get(self, conn: Connection, contract, end: date, length: timedelta, interval_min=5)->IIVALookup:
        """
        算出來是1分K的平均值(之後評估加不同分鐘數的)
        :param end:
        :param length:
        :param interval_min:
        :return:
        """
        s_date = (end - length)
        symbol = contract.symbol

        key = IntradayIntervalVolumeAvg2._npy_cache_key(symbol, end, length, interval_min)
        if CacheState.MISS == npy_cache_manager.is_cached(key):
            slices = self.kbar_manager.get(conn, contract, s_date, end)
            if slices:
                kbars = NPKBars.merge_slices([s.np_slice for s in slices])
                data = self._calc(kbars, interval_min)
                np_data = self._to_ndarray(data, interval_min)
                npy_cache_manager.set(key, np_data)
            else:
                np_data = None
                npy_cache_manager.mark_empty(key)
        elif CacheState.EMPTY == npy_cache_manager.is_cached(key):
            np_data = None
        else:
            np_data = self._arr_from_npy(key)

        lookup = IIVALookup(np_data,interval_min)

        return lookup
