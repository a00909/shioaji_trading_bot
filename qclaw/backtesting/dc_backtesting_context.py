"""
Donchian 回測情境基礎類別
"""
import sys
import time

from tools.backtesting_context import BacktestingContext

sys.path.insert(0, r'/')

import numpy as np
import pandas as pd

from datetime import timedelta

from qclaw_bk.ml_strategy.feature_builder import FeatureBuilder, FeatureConfig
import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import matplotlib.dates as mdates

import sys

sys.path.insert(0, r'C:\Repos\shioaji_trading')


class DonchianBacktestingContext(BacktestingContext):
    def __init__(self, test_dates):
        super().__init__()
        self.TEST_DATES = test_dates

        arr_times, arr_prices, arr_ha, arr_la, arr_h, arr_l = [], [], [], [], [], []
        day_labels = []
        self.day_ranges = {}
        for date_str in self.TEST_DATES:
            _st_time = time.time()
            ticks = self.htm.get_data(self.contract, date_str)
            print(f'tick load consumed {time.time() - _st_time} seconds.')

            if not ticks:
                print(f'  [skip] {date_str}')
                continue

            _st_time = time.time()
            fb = FeatureBuilder(ticks, self.iiva_lookup, FeatureConfig(
                donchian_window=timedelta(seconds=1800),
            ))
            f = fb.build(['price', 'donchian_ha', 'donchian_la', 'donchian_h', 'donchian_l'])
            print(f'feature build consumed {time.time() - _st_time} seconds.')

            arr_times.append(fb.times.astype(np.float64))
            arr_prices.append(f['price'].values.astype(np.float64))
            arr_ha.append(f['donchian_ha'].values.astype(np.float64))
            arr_la.append(f['donchian_la'].values.astype(np.float64))
            arr_h.append(f['donchian_h'].values.astype(np.float64))
            arr_l.append(f['donchian_l'].values.astype(np.float64))
            day_labels.extend([date_str] * len(fb.times))
            self.day_ranges[date_str] = float(f['price'].values.max() - f['price'].values.min())

            print(f'  {date_str}: {len(fb.times)} ticks')

        self.times = np.concatenate(arr_times)
        self.prices = np.concatenate(arr_prices)
        self.has = np.concatenate(arr_ha)
        self.las = np.concatenate(arr_la)
        self.hs = np.concatenate(arr_h)
        self.ls = np.concatenate(arr_l)
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
