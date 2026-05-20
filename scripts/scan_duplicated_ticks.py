from datetime import timedelta, time

from tools.app.app import App

app = App()
duplicated_dates = []
duplicated_ids = []

ts_anomaly_dates = []
ts_anomaly_ids = []

with app.engine.raw_connection() as raw_conn:
    conn = raw_conn.driver_connection
    with conn.cursor(name='tick_fast_stream') as cur:

        query = """
            SELECT ts, id
            FROM history_tick 
            ORDER BY ts ASC, id ASC;
        """
        cur.execute(query)

        prev_id = None
        prev_ts = None
        skip_current_date = False

        print("start scanning.\n")

        for row in cur:
            current_ts, current_id = row
            current_time = current_ts.time()

            if prev_ts and skip_current_date and current_ts - prev_ts > timedelta(minutes=30):
                skip_current_date = False

            # 判斷日期誤植
            if (
                    current_ts.weekday() == 5 and current_ts.hour >= 5
                    or current_ts.weekday() == 6
            ):
                if not ts_anomaly_dates or current_ts.date() != ts_anomaly_dates[-1].date():
                    print(f"ts anomaly ticks detected.")
                    print(f"  -> at: {current_ts} (weekday: {current_ts.weekday()})")
                    print(f"  -> prev_ts: {prev_ts}")
                    print(f"  -> prev_id: {prev_id}")
                    print(f"  -> curr_id: {current_id}")
                    ts_anomaly_dates.append(current_ts)
                    ts_anomaly_ids.append(current_id)

            if prev_id and not skip_current_date and current_id - prev_id != 1 and current_ts - prev_ts <= timedelta(
                    seconds=10):
                print(f"duplicated ticks detected.")
                print(f"  -> at: {current_ts} (weekday: {current_ts.weekday()})")
                print(f"  -> prev_ts: {prev_ts}")
                print(f"  -> prev_id: {prev_id}")
                print(f"  -> curr_id: {current_id}")
                print(f"  -> skip scanning until market close.\n")
                duplicated_dates.append(current_ts)
                duplicated_ids.append(current_id)

                # 進入 skip 狀態直到前後兩筆間隔至少30分以上
                skip_current_date = True

            prev_id = current_id
            prev_ts = current_ts

        print("scan finished.")

        if duplicated_dates:
            print('duplicated ticks:')
            for dt, id_ in zip(duplicated_dates, duplicated_ids):
                print(dt, dt.weekday(), id_)
            print()

        if ts_anomaly_dates:
            print('timestamp anomaly ticks:')
            for dt, id_ in zip(ts_anomaly_dates, ts_anomaly_ids):
                print(dt, dt.weekday(), id_)
            print()

        query = """
            SELECT id 
            FROM history_tick 
            WHERE ts BETWEEN %s and %s
            ORDER BY ts ASC, id ASC;
        """

        id_ranges_to_be_removed = []
        for dt in duplicated_dates:
            if time(15, 0, 0) <= dt.time() <= time(15, 10, 0):
                cur.execute(query, (
                    dt,
                    (dt + timedelta(days=1)).replace(hour=13, minute=45, second=5)
                ))
            elif time(8, 45, 0) <= dt.time() <= time(8, 55, 0):
                cur.execute(query, (
                    dt,
                    dt.replace(hour=13, minute=45, second=5)
                ))

            # 根據id連續的特性分類
            fst_id_to_all_id_lst = {}

            for id_, in cur:
                if id_ - 1 in fst_id_to_all_id_lst:
                    fst_id_to_all_id_lst[id_] = fst_id_to_all_id_lst.pop(id_ - 1)
                    fst_id_to_all_id_lst[id_].append(id_)
                else:
                    fst_id_to_all_id_lst[id_] = [id_]

            # 檢查每包長度是否相同，若有不同則提示需手動確認
            len_ = -1
            skip_this_round = False
            smallest_end_id = -1
            for end_id, ids in fst_id_to_all_id_lst.items():
                if smallest_end_id == -1:
                    smallest_end_id = end_id
                else:
                    smallest_end_id = min(smallest_end_id, end_id)

                if len_ == -1:
                    len_ = len(ids)
                    continue
                if len_ != len(ids):
                    print(f'wrong length with id: {ids[0]}')
                    print(f'skip this day. should be checked manually.\n')
                    skip_this_round = True
                    break

            # 提示需要移除的id和需要保留的id
            if not skip_this_round:
                print('ticks should be removed(id):')
                for end_id, ids in fst_id_to_all_id_lst.items():
                    if end_id == smallest_end_id:
                        print(f'{ids[0]}-{ids[-1]}({ids[-1] - ids[0] + 1} items): should be keep')
                    else:
                        print(f'{ids[0]}-{ids[-1]}({ids[-1] - ids[0] + 1} items): should be removed')
                        id_ranges_to_be_removed.append((ids[0], ids[-1]))
                print()

    if not id_ranges_to_be_removed:
        print('no data to be removed.')
    else:
        with conn.cursor() as cur:
            if 'YES' == input('ARE YOU SURE TO REMOVE ABOVE ITEMS? please type YES(uppercased).\n'):

                rm_sql = """
                    DELETE FROM history_tick WHERE id between %s and %s
                """
                for st, ed in id_ranges_to_be_removed:
                    cur.execute(rm_sql, (st, ed))
                    print(f'{st}-{ed} removed: {cur.rowcount} row')

                conn.commit()
                print('done')
            else:
                print('process interrupted.')
