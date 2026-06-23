"""
Donchian 回測情境基礎類別
"""
import sys
import time

from data_manager.history.statics.tick.np_ticks import NPTicks
from data_manager.history.statics.data import DailySlice
from tools.backtesting_context import BacktestingContext

sys.path.insert(0, r'/')

import numpy as np
import pandas as pd

from datetime import timedelta, date

from backtesting.feature_builder.feature_builder import FeatureBuilder
from backtesting.feature_builder._feature_config import FeatureConfig

import sys

sys.path.insert(0, r'C:\Repos\shioaji_trading')


class DonchianBacktestingContext(BacktestingContext):
    def __init__(self, start: date, end: date, with_label=False):
        super().__init__()

        day_labels = []
        self.day_ranges = {}

        # init ticks
        _st_time = time.time()
        with self.app.raw_connection as conn:
            daily_slices: list[DailySlice] = self.npy_htm.get(conn, self.contract, start, end)
        slices: list[NPTicks] = [s.np_slice for s in daily_slices if s.np_slice is not None]
        ticks = NPTicks.merge_slices(slices)

        print(f'tick load consumed {time.time() - _st_time} seconds.')

        # build features
        _st_time = time.time()
        fb = FeatureBuilder(ticks, self.iiva_lookup, FeatureConfig(
            donchian_window=timedelta(seconds=1800),
        ))
        f = fb.build_features(['price', 'donchian_ha', 'donchian_la', 'donchian_h', 'donchian_l'], with_label)
        print(f'feature build consumed {time.time() - _st_time} seconds.')

        for daily_slice in daily_slices:
            day_labels.extend([daily_slice.date] * len(daily_slice.np_slice))
            daily_max = np.max(daily_slice.np_slice.close)
            daily_min = np.min(daily_slice.np_slice.close)
            self.day_ranges[daily_slice.date] = float(daily_max - daily_min)

            print(f'\t{daily_slice.date}: {len(daily_slice.np_slice)}')

        self.times = fb.times.astype(np.float64)
        self.prices = f['price'].astype(np.float64)
        self.has = f['donchian_ha'].astype(np.float64)
        self.las = f['donchian_la'].astype(np.float64)
        self.hs = f['donchian_h'].astype(np.float64)
        self.ls = f['donchian_l'].astype(np.float64)
        self.days = np.array(day_labels, dtype=object)
        self.n_total = len(self.prices)
        self.vol = ticks.volume
        self.features = f

        # datetime index
        idx_raw = pd.to_datetime(self.times, unit='s')
        counter = pd.Series(idx_raw).groupby(idx_raw).cumcount()
        self.idx = pd.DatetimeIndex(idx_raw) + pd.to_timedelta(counter * pd.Timedelta('1ns'))
        print(f'\ntotal: {self.n_total} ticks, {len(self.day_ranges)} days\n')
        self.app.shut()

    def _get_tick_data(self, i):
        entry_price = self.prices[i]
        entry_time = self.idx[i]
        entry_day = self.days[i]
        return entry_price, entry_time, entry_day
