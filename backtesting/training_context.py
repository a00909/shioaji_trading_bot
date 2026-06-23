from datetime import timedelta

import numpy as np

from backtesting.feature_builder._feature_config import FeatureConfig
from backtesting.feature_builder.feature_builder import FeatureBuilder
from data_manager.history.statics.aggregated_bars import AggregatedBars
from data_manager.history.statics.tick.np_ticks import NPTicks
from strategy.tools.kbar_indicators.intraday_interval_volume_avg.iiva2 import IntradayIntervalVolumeAvg2
from tools.backtesting_context import BacktestingContext
from tools.utils import tmf_r1_contract


class TrainingContext(BacktestingContext):
    def __init__(self, start, end, feature_names, with_label=False, gen_bars=False, bar_interval_seconds=1,
                 use_feature_mtx=False):
        super().__init__()
        self.bars = None
        self.ticks = None
        self.features: dict[str, np.ndarray] | np.ndarray = None
        self.label = None
        self.valid = None

        with self.app.raw_connection as conn:
            daily_slices = self.npy_htm.get(conn, tmf_r1_contract(self.app.api), start, end)

            iiva_lookups = {}


            for s in daily_slices:
                if s.np_slice:
                    for i in range (-1,1):
                        dt_need = s.date + timedelta(days=i)
                        if dt_need not in iiva_lookups:
                            iiva_lookup = self.iiva.get(
                                conn,
                                tmf_r1_contract(self.app.api),
                                dt_need-timedelta(days=1), # t要取t-1的資料
                                timedelta(days=5),
                                interval_min=360
                            )
                            iiva_lookups[dt_need] = iiva_lookup

        self.app.shut()


        slices: list[NPTicks] = [s.np_slice for s in daily_slices if s.np_slice is not None]
        self.ticks = NPTicks.merge_slices(slices)
        print('ticks load.')

        if gen_bars:
            self.bars = AggregatedBars.from_npticks(self.ticks, bar_interval_seconds)
            print('bars aggregated.')

        data = self.bars or self.ticks
        self.fb = FeatureBuilder(data, iiva_lookups, FeatureConfig())
        self.features = self.fb.build_features(
            feature_names,
            return_mtx=use_feature_mtx,
        )
        if with_label:
            self.label,self.valid = self.fb.build_label()

