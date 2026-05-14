from dataclasses import asdict
from datetime import datetime, date

from pandas import DataFrame

from data_manager.history_data_manager.history_tick_manager import HistoryTickManager
from qclaw.backtesting.npy_cached_history_tick_manager import NpyCachedHistoryTickManager, TickSlice
from qclaw.backtesting.npy_cache import NpyCacheManager
from tools.plotter import plotter
from tools.app import App

app = App(True)
npy_cache_manager = NpyCacheManager()
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
npy_cached_htm = NpyCachedHistoryTickManager(npy_cache_manager, app.history_tick_manager)
slice_ = npy_cached_htm.get(
    app.api.Contracts.Futures.TMF.TMFR1,
    date(2026, 5, 7),
    date(2026, 5, 8),
)
app.shut()

slice_ = list(s.tick_slice for s in slice_)
df = DataFrame(asdict(
    TickSlice.merge_slices(slice_)
))
print(df)

slice_len = len(slice_.times)
plotter.active()
for i in range(slice_len):
    plotter.add_points(
        'price',
        (datetime.fromtimestamp(slice_.times[i]), slice_.closes[i])
    )
    plotter.add_points(
        'vol',
        (datetime.fromtimestamp(slice_.times[i]), slice_.volumes[i]),
        chart_idx=1
    )

plotter.plot()

pass
