import datetime

from shioaji.constant import TicksQueryType

from tools import get_api

# api = get_api(True)
#
# print(api.Contracts.Futures.TMF.TMFR1)
# name = api.Contracts.Futures.TMF.TMFR1.symbol
# print(name)
#
# pass
# ticks = api.ticks(
#     api.Contracts.Futures.TMF.TMFR1,
#     '2024-11-02',
#     TicksQueryType.AllDay,
#
# )
#
import pandas as pd

# df = pd.DataFrame({**ticks})
# df.ts = pd.to_datetime(df.ts)
# print(df.head(50))
#
# api.logout()
data = [
    {'ts': 1235, 'data': 'abc'},
    {'ts': 1236, 'data': 'efg'},
    {'ts': 1237, 'data': 'hig'},
    {'ts': 1238, 'data': 'kml'},
]
df = pd.DataFrame(data)
print(df.head())
