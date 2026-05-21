from contextlib import closing
from datetime import date

from data_manager.history.htm2._db_loader import DBLoader
from tools.app.app import App
from tools.plotter import plotter
from tools.time_utils import pg_us_to_datetime

app = App()
# contract = tmf_r1_contract(app.api)
symbol = 'TMFR1'
db_loader = DBLoader()

with closing(app.engine.raw_connection()) as conn:
    db_data = db_loader.load(conn, symbol, {date(2026, 5, 21)})

plotter.active()

for dt, ticks in db_data.items():
    tick_len = len(ticks.ts)
    print(f'{dt}: {tick_len}')

    for i in range(tick_len):
        dtm = pg_us_to_datetime(ticks.ts[i])

        plotter.add_points(
            'price',
            (dtm, ticks.close[i])
        )
        plotter.add_points(
            'vol',
            (dtm, ticks.volume[i]),
            chart_idx=1
        )
    print(f'start: {pg_us_to_datetime(ticks.ts[0])}\n'
          f'end: {pg_us_to_datetime(ticks.ts[-1])}\n'
          )

plotter.plot()
