from bisect import bisect_left, bisect_right
from datetime import date, timedelta, datetime

from shioaji.constant import TicksQueryType

from data_manager.history.htm2._api_fetcher import ApiFetcher
from tools.app.app import App
from tools.utils import tmf_r1_contract
from tools.time_utils import sj_history_ns_to_datetime, pg_us_to_datetime, sj_history_ns_to_pg_us, \
    datetime_to_pg_us

app = App()
api = app.api
contract = tmf_r1_contract(api)
start = date(2026, 4, 15)
ticks = api.ticks(
    contract,
    start.strftime("%Y-%m-%d"),
    TicksQueryType.AllDay,
    # time(0, 0, 0).strftime("%H:%M:%S"),
    # time(23, 59, 59).strftime("%H:%M:%S"),
)

st = ticks.ts[0]
ed = ticks.ts[-1]

print(st, sj_history_ns_to_datetime(st))
print(ed, sj_history_ns_to_datetime(ed))
print(len(ticks.ts))

print('check overlap')
ticks.ts = [sj_history_ns_to_pg_us(ns) for ns in ticks.ts]
pre_start = start - timedelta(days=1)

st1 = ticks.ts[0]
ed1 = ticks.ts[-1]
st2 = datetime_to_pg_us(datetime(pre_start.year, pre_start.month, pre_start.day, 15, 00, 00))
ed2 = datetime_to_pg_us(datetime(start.year, start.month, start.day, 13, 45, 5))
is_ov, rng = ApiFetcher._get_overlap_interval(st1, ed1, st2, ed2)

if is_ov:
    st_ov, ed_ov = rng

    print(f'required range: {pg_us_to_datetime(st2)}, {pg_us_to_datetime(ed2)}')
    print(f'api returned range: {pg_us_to_datetime(st1)}, {pg_us_to_datetime(ed1)}')
    print(f'overlapped range: {pg_us_to_datetime(st_ov)}, {pg_us_to_datetime(ed_ov)}')

    left_idx = bisect_left(ticks.ts, st_ov)
    right_idx = bisect_right(ticks.ts, ed_ov)

    print(
        f'final range: ('
        f'{pg_us_to_datetime(ticks.ts[left_idx])}({left_idx}), '
        f'{pg_us_to_datetime(ticks.ts[right_idx - 1])}({right_idx - 1})'
        f'), '
        f'{len(ticks.ts[left_idx:right_idx])} items.'
    )
