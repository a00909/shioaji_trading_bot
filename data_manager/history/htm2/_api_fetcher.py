from bisect import bisect_left, bisect_right
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from queue import Queue
from threading import Thread

from psycopg import Connection
from shioaji import Shioaji
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks

from data_manager.history.htm2._npy_binary_packer import npy_pack
from data_manager.history.htm2._partition_creator import create_partition_table_2
from data_manager.history.statics.np_ticks import NPTicks
from data_manager.history.statics.tick_field import TICKS_FIELDS
from database.schema.history_tick import HistoryTick
from tools.logger.custom_logger import CustomLogger
from tools.time_utils import pg_us_to_datetime, datetime_to_sj_ns


class ApiFetcher:
    COPY_TICK_SQL = f"COPY history_tick ({', '.join(TICKS_FIELDS)}, symbol) FROM STDIN (FORMAT BINARY)"
    INSERT_MEMO_SQL = "INSERT INTO history_tick_memo (date, symbol) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    COPY_BINARY_HEADER = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00"
    COPY_BINARY_TRAILER = b"\xff\xff"

    def __init__(self, api):
        self._api: Shioaji = api
        self._logger = CustomLogger.get_logger('api_fetcher')

    @staticmethod
    def _get_overlap_interval(st1, ed1, st2, ed2):
        st_overlap = max(st1, st2)
        ed_overlap = min(ed1, ed2)

        if st_overlap <= ed_overlap:
            return True, (st_overlap, ed_overlap)  # 有交集，回傳範圍
        else:
            return False, None  # 無交集

    @staticmethod
    def _slice_ticks_inplace(ticks: Ticks, start: int, end: int) -> None:
        for f in TICKS_FIELDS:
            ticks.__dict__[f][:] = ticks.__dict__[f][start:end]

    def _fetch_single(self, contract, start: date, data_queue: Queue):
        ticks = self._api.ticks(
            contract,
            start.strftime('%Y-%m-%d'),
            TicksQueryType.AllDay,
        )

        if ticks.ts:

            # 判斷取回資料跟所需資料的交集範圍
            pre_start = start - timedelta(days=1)
            st1 = ticks.ts[0]
            ed1 = ticks.ts[-1]
            st2 = datetime_to_sj_ns(datetime(pre_start.year, pre_start.month, pre_start.day, 15, 00, 00))
            ed2 = datetime_to_sj_ns(datetime(start.year, start.month, start.day, 13, 45, 5))
            is_ov, rng = ApiFetcher._get_overlap_interval(st1, ed1, st2, ed2)

            if is_ov:  # 有交集
                st_ov, ed_ov = rng
                if st_ov == st1 and ed_ov == ed1:  # 如果取交集範圍 == api 回傳資料範圍則無須切片
                    pass
                else:
                    left_idx = bisect_left(ticks.ts, st_ov)
                    right_idx = bisect_right(ticks.ts, ed_ov)
                    ApiFetcher._slice_ticks_inplace(ticks, left_idx, right_idx)
                np_ticks = NPTicks.from_ticks(ticks)
            else:
                np_ticks = None
        else:
            np_ticks = None

        single = (start, np_ticks)

        data_queue.put(single)
        return single

    def _to_db(self, conn: Connection, symbol: str, data_queue: Queue):
        while True:
            item: None | tuple = data_queue.get()
            if item is None:
                data_queue.task_done()
                break

            dt: date
            ticks: Ticks
            dt, ticks = item

            with conn.cursor() as cur:
                with cur.copy(ApiFetcher.COPY_TICK_SQL) as copy:
                    if ticks:
                        copy.write(ApiFetcher.COPY_BINARY_HEADER)
                        npy_bytes = npy_pack(symbol, ticks)
                        copy.write(npy_bytes)
                        copy.write(ApiFetcher.COPY_BINARY_TRAILER)
                        self._logger.info(f'({symbol}, {dt.strftime('%Y-%m-%d')}): {len(ticks.ts)} items.')
                        self._logger.info(
                            f'range: {pg_us_to_datetime(ticks.ts[0])} to {pg_us_to_datetime(ticks.ts[-1])}'
                        )
                    else:
                        self._logger.info('no tick has been written.')

                cur.execute(ApiFetcher.INSERT_MEMO_SQL, (dt, symbol))

            conn.commit()
            data_queue.task_done()

    @staticmethod
    def _create_partition(conn: Connection, dates):
        with conn.cursor() as cur:
            create_partition_table_2(cur, HistoryTick.__tablename__, dates)
        conn.commit()

    def fetch(self, conn: Connection, contract, dates: set[date]) -> dict[date, NPTicks]:
        symbol = contract.symbol
        ApiFetcher._create_partition(conn, dates)

        data_queue = Queue(maxsize=5)
        db_thread = Thread(target=self._to_db, args=(conn, symbol, data_queue))
        db_thread.start()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {dt: executor.submit(self._fetch_single, contract, dt, data_queue) for dt in dates}
        for k, v in futures.items():
            futures[k] = v.result()[1]

        data_queue.put(None)

        db_thread.join()
        data_queue.join()

        return futures
