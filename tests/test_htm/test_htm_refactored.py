from data_manager.history_data_manager.history_tick_manager import DailyTicks
from tools.backtesting_context import BacktestingContext
from tools.plotter import plotter

bc = BacktestingContext()

daily_ticks: list[DailyTicks] = bc.htm.get_data_batch(bc.contract, '2026-05-08', '2026-05-09')
bc.shut()

for d in daily_ticks:
    print(d.date, len(d.ticks))

plotter.active()
for _daily_ticks in daily_ticks:
    for t in _daily_ticks.ticks:
        plotter.add_points(
            'price',
            (t.ts, t.close)
        )
        plotter.add_points(
            'vol',
            (t.ts, t.volume),
            chart_idx=1
        )

plotter.plot()
