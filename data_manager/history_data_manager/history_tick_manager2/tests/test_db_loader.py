from datetime import date, datetime

from data_manager.history_data_manager.history_tick_manager2.db_loader import DBLoader
from tools.app import App
from tools.plotter import plotter
from tools.utils import tmf_r1_contract, db_history_ts_to_datetime

app = App()
# contract = tmf_r1_contract(app.api)
symbol = 'TMFR1'
db_loader = DBLoader(app.engine)

db_data = db_loader.load(symbol, {date(2026, 2, 11)})

plotter.active()
for dt, ticks in db_data.items():
    tick_len = len(ticks.ts)
    print(f'{dt}: {tick_len}')

    for i in range(tick_len):
        dtm = db_history_ts_to_datetime(ticks.ts[i])
        plotter.add_points(
            'price',
            (dtm, ticks.close[i])
        )
        plotter.add_points(
            'vol',
            (dtm, ticks.volume[i]),
            chart_idx=1
        )

plotter.plot()

