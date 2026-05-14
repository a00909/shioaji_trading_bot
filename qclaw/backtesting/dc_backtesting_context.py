"""
Donchian 回測情境基礎類別
"""
import sys
import time

from qclaw.backtesting.npy_cached_history_tick_manager import DailySlice, TickSlice
from tools.backtesting_context import BacktestingContext

sys.path.insert(0, r'/')

import numpy as np
import pandas as pd

from datetime import timedelta, date

from qclaw_bk.ml_strategy.feature_builder import FeatureBuilder, FeatureConfig
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import matplotlib.dates as mdates

import sys

sys.path.insert(0, r'C:\Repos\shioaji_trading')


class DonchianBacktestingContext(BacktestingContext):
    def __init__(self, start: date = None, end: date = None):
        super().__init__()

        day_labels = []
        self.day_ranges = {}

        # init ticks
        _st_time = time.time()
        daily_slices: list[DailySlice] = self.npy_htm.get(self.contract, start, end)
        slices: list[TickSlice] = [s.tick_slice for s in daily_slices if s.tick_slice is not None]
        ticks = TickSlice.merge_slices(slices)

        print(f'tick load consumed {time.time() - _st_time} seconds.')

        # build features
        _st_time = time.time()
        fb = FeatureBuilder(ticks, self.iiva_lookup, FeatureConfig(
            donchian_window=timedelta(seconds=1800),
        ))
        f = fb.build(['price', 'donchian_ha', 'donchian_la', 'donchian_h', 'donchian_l'])
        print(f'feature build consumed {time.time() - _st_time} seconds.')

        for daily_slice in daily_slices:
            day_labels.extend([daily_slice.date] * len(daily_slice.tick_slice))
            daily_max = np.max(daily_slice.tick_slice.closes)
            daily_min = np.min(daily_slice.tick_slice.closes)
            self.day_ranges[daily_slice.date] = float(daily_max - daily_min)

            print(f'\t{daily_slice.date}: {len(daily_slice.tick_slice)}')

        self.times = fb.times.astype(np.float64)
        self.prices = f['price'].values.astype(np.float64)
        self.has = f['donchian_ha'].values.astype(np.float64)
        self.las = f['donchian_la'].values.astype(np.float64)
        self.hs = f['donchian_h'].values.astype(np.float64)
        self.ls = f['donchian_l'].values.astype(np.float64)
        self.days = np.array(day_labels, dtype=object)
        self.n_total = len(self.prices)

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
