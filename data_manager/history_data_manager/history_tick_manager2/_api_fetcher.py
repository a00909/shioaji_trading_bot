import itertools
import struct
from bisect import bisect_left, bisect_right
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from queue import Queue
from threading import Thread
from typing import cast

import psycopg
from shioaji import Shioaji
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks
from sqlalchemy import Engine

from data_manager.history_data_manager.history_tick_manager2._common import TICKS_FIELDS
from data_manager.history_data_manager.history_tick_manager2._partition_creator import create_partition_table_2
from data_manager.history_data_manager.statics.data import DailyTicks
from database.schema.history_tick import HistoryTick
from tools.logger.custom_logger import CustomLogger
from tools.time_utils import sj_history_ns_to_pg_us, datetime_to_pg_us, pg_us_to_datetime


class ApiFetcher:
    COPY_TICK_SQL = f"COPY history_tick ({', '.join(TICKS_FIELDS)}, symbol) FROM STDIN (FORMAT BINARY)"
    INSERT_MEMO_SQL = "INSERT INTO history_tick_memo (date, symbol) VALUES (%s, %s)"
    PACK_FMT_TEMPLATE = '!h Iq Id Ii Id Ii Id Ii Ii I{0}s'
    FIELD_NUM = len(TICKS_FIELDS) + 1
    FIELD_LENS = (8, 8, 4, 8, 4, 8, 4, 4)
    COPY_BINARY_HEADER = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00"
    COPY_BINARY_TRAILER = b"\xff\xff"

    def __init__(self, api, engine):
        self.api: Shioaji = api
        self.engine: Engine = engine
        self._data_queue: Queue = None
        self._symbol = None
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
    def _slice_ticks_inplace(ticks_obj: Ticks, start: int, end: int) -> None:
        """直接修改傳入的 Ticks 物件內部的所有 List 內容（不建立新物件）"""
        fields = getattr(ticks_obj, "model_fields", getattr(ticks_obj, "__fields__", {}))

        for field_name in fields:
            lst = getattr(ticks_obj, field_name)
            # 利用 [:] 原地覆蓋，記憶體地址不變，但內容被切片了
            lst[:] = lst[start:end]

    def _fetch_single(self, contract, start: date):
        ticks = self.api.ticks(
            contract,
            start.strftime('%Y-%m-%d'),
            TicksQueryType.AllDay,
        )
        ticks.ts = [sj_history_ns_to_pg_us(ns) for ns in ticks.ts]

        # 判斷取回資料跟所需資料的交集範圍
        pre_start = start - timedelta(days=1)
        st1 = ticks.ts[0]
        ed1 = ticks.ts[-1]
        st2 = datetime_to_pg_us(datetime(pre_start.year, pre_start.month, pre_start.day, 15, 00, 00))
        ed2 = datetime_to_pg_us(datetime(start.year, start.month, start.day, 13, 45, 5))
        is_ov, rng = ApiFetcher._get_overlap_interval(st1, ed1, st2, ed2)

        if is_ov:  # 有交集
            st_ov, ed_ov = rng
            if st_ov == st1 and ed_ov == ed1:  # 如果取交集範圍 == api 回傳資料範圍則無須切片
                pass
            else:
                left_idx = bisect_left(ticks.ts, st_ov)
                right_idx = bisect_right(ticks.ts, ed_ov)
                ApiFetcher._slice_ticks_inplace(ticks, left_idx, right_idx)
        else:
            ticks = None

        single = DailyTicks(start, ticks)

        self._data_queue.put(single)
        return single

    @staticmethod
    def _data_generator(ticks: Ticks):
        # 1. 在迴圈外只呼叫一次 getattr，把所有欄位的 List 先拿出來
        # columns 的結構會是: [ticks.ts, ticks.close, ticks.volume, ...]
        columns = [getattr(ticks, f) for f in TICKS_FIELDS]

        # 2. 利用 zip(*columns) 動態解包，底層會像 C 指標一樣同時橫向尋訪
        # 這會直接回傳一個高效的 Generator 物件
        return zip(*columns)

    def _to_db(self):
        with self.engine.raw_connection() as raw_conn:
            conn = cast(psycopg.Connection, raw_conn.connection)

            sym_len = len(self._symbol)
            fmt = ApiFetcher.PACK_FMT_TEMPLATE.format(sym_len)
            sym_bytes = self._symbol.encode()
            overall_field_lens = ApiFetcher.FIELD_LENS + (sym_len,)

            while True:
                item: DailyTicks = self._data_queue.get()
                if item is None:
                    self._data_queue.task_done()
                    break

                dt = item.date
                ticks: Ticks = item.ticks

                with conn.cursor() as cur:
                    with cur.copy(ApiFetcher.COPY_TICK_SQL) as copy:
                        if ticks:
                            copy.write(ApiFetcher.COPY_BINARY_HEADER)
                            for e, row in enumerate(ApiFetcher._data_generator(ticks)):
                                values = itertools.chain(row, (sym_bytes,))
                                size_value_flat = itertools.chain.from_iterable(zip(overall_field_lens, values))
                                raw = struct.pack(fmt, ApiFetcher.FIELD_NUM, *size_value_flat)
                                copy.write(raw)
                            copy.write(ApiFetcher.COPY_BINARY_TRAILER)
                            self._logger.info(f'({self._symbol}, {dt.strftime('%Y-%m-%d')}): {len(ticks.ts)} items.')
                            self._logger.info(
                                f'range: {pg_us_to_datetime(ticks.ts[0])} to {pg_us_to_datetime(ticks.ts[-1])}'
                            )
                        else:
                            self._logger.info('no tick has been written.')

                    cur.execute(ApiFetcher.INSERT_MEMO_SQL, (dt, self._symbol))

                conn.commit()
                self._data_queue.task_done()

    def _create_partition(self, dates):
        with self.engine.raw_connection() as raw_conn:
            conn = cast(psycopg.Connection, raw_conn.connection)
            with conn.cursor() as cur:
                create_partition_table_2(cur, HistoryTick.__tablename__, dates)
            conn.commit()

    def fetch(self, contract, dates: set[date]) -> dict[date, DailyTicks]:
        self._symbol = contract.symbol
        self._create_partition(dates)
        self._data_queue = Queue(maxsize=5)
        db_thread = Thread(target=self._to_db)
        db_thread.start()

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {dt: executor.submit(self._fetch_single, contract, dt) for dt in dates}
        for k, v in futures.items():
            futures[k] = v.result()

        self._data_queue.put(None)

        db_thread.join()
        self._data_queue.join()
        self._data_queue = None

        return futures
