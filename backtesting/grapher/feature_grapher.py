from backtesting.feature_builder._feature_config import FeatureConfig
from backtesting.feature_builder.feature_builder import FeatureBuilder
from backtesting.feature_builder.feature_name import FeatureName
from data_manager.history.statics.tick.np_ticks import NPTicks
from tools.backtesting_context import BacktestingContext
from tools.plotter import Plotter
from tools.utils import tmf_r1_contract


class FeatureGrapher(BacktestingContext):
    def graph(self, start, end, feature_to_graph_idx: dict[FeatureName | str, int]):
        with self.app.raw_connection as conn:
            daily_slices = self.npy_htm.get(conn, tmf_r1_contract(self.app.api), start, end)
        slices: list[NPTicks] = [s.np_slice for s in daily_slices if s.np_slice is not None]

        ticks = NPTicks.merge_slices(slices)

        print('ticks load.')

        fb = FeatureBuilder(ticks, self.iiva_lookup, FeatureConfig())

        f = fb.build_features(list(feature_to_graph_idx.keys()))

        times = ticks.datetime64()

        plotter = Plotter(True)

        for i in range(len(ticks)):
            for n, idx in feature_to_graph_idx.items():
                plotter.add_points(
                    n,
                    (times[i], f[n][i]),
                    chart_idx=idx
                )
        plotter.plot()
