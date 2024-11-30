import datetime
import threading

from shioaji.constant import TicksQueryType
from shioaji.data import Ticks

from utils.app import App
import pandas as pd
from datetime import datetime, timedelta

from utils.constants import DATE_FORMAT_SHIOAJI

app = App(True)
contract = app.api.Contracts.Futures.TMF.TMFR1
date = (datetime.now().date()+timedelta(days=1)).strftime(DATE_FORMAT_SHIOAJI)

# htm = HistoryTickManager(app.api, app.redis, app.session_maker)
# ticks = htm.get_tick(app.api.Contracts.Futures.TMF.TMFR1, '2024-11-21')
# print('data ok')
# df = pd.DataFrame([t.to_dict() for t in ticks])
#
# # 設定顯示所有行
# pd.set_option('display.max_rows', 10)
#
# # 設定顯示所有列而不自動換行
# pd.set_option('display.max_colwidth', None)
#
# # 設定顯示所有列
# pd.set_option('display.max_columns', None)
# print(df)

got_tick_event = threading.Event()


def cb(ticks: Ticks):
    df = pd.DataFrame({**h_ticks})
    df.ts = pd.to_datetime(df.ts)
    df.head()

    print(df)
    got_tick_event.set()


h_ticks = app.api.ticks(
    contract,
    date,
    TicksQueryType.LastCount,
    timeout=0,
    last_cnt=20,
    cb=cb
)


print('wait for callback...')
got_tick_event.wait()

app.shut()
