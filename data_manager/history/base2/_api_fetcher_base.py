from abc import abstractmethod, ABC
from bisect import bisect_left, bisect_right
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from queue import Queue
from threading import Thread

from psycopg import Connection

from data_manager.history.statics.base._history_data_spec import _HistoryDataSpec
from data_manager.history.common._npy_binary_packer import npy_pack
from data_manager.history.common._partition_creator import create_partition_table_2
from data_manager.history.common._ts_data_protocol import TsData
from data_manager.history.statics.base._np_data_base import _NpDataBase
from tools.logger.custom_logger import CustomLogger
from tools.time_utils import datetime_to_sj_ns, pg_us_to_datetime


class _ApiFetcherBase[D:_NpDataBase](ABC):
    COPY_BINARY_HEADER = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00"
    COPY_BINARY_TRAILER = b"\xff\xff"
    _COPY_TICK_SQL_TEMPL = "COPY {0} ({1}, symbol) FROM STDIN (FORMAT BINARY)"
    _INSERT_MEMO_SQL_TEMPL = "INSERT INTO {0}_memo (date, symbol) VALUES (%s, %s) ON CONFLICT DO NOTHING"

    def __init__(self, api, data_spec: _HistoryDataSpec):
        self._api = api
        self._logger = CustomLogger.get_logger(f'{data_spec.logger_prefix}_api_fetcher')
        self._data_spec: _HistoryDataSpec = data_spec

    @staticmethod
    def _get_overlap_interval(st1, ed1, st2, ed2):
        st_overlap = max(st1, st2)
        ed_overlap = min(ed1, ed2)

        if st_overlap <= ed_overlap:
            return True, (st_overlap, ed_overlap)  # 有交集，回傳範圍
        else:
            return False, None  # 無交集

    def _slice_ticks_inplace(self, np_data: D, start: int, end: int) -> None:
        for f in self._data_spec.field_enum.names():
            np_data.__dict__[f][:] = np_data.__dict__[f][start:end]

    def _create_partition(self, conn: Connection, dates):
        with conn.cursor() as cur:
            create_partition_table_2(cur, self._data_spec.table_name, dates)
        conn.commit()

    def fetch(self, conn: Connection, contract, dates: set[date]) -> dict[date, D]:
        symbol = contract.symbol
        self._create_partition(conn, dates)

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

    def _fetch_single(self, contract, start: date, data_queue: Queue):
        raw: D = self._fetch_from_api(contract, start)
        if raw.ts:

            # 判斷取回資料跟所需資料的交集範圍
            st1 = raw.ts[0]
            ed1 = raw.ts[-1]
            st2, ed2 = self._data_spec.daily_time_range(start)
            st2 = datetime_to_sj_ns(st2)
            ed2 = datetime_to_sj_ns(ed2)
            is_ov, rng = _ApiFetcherBase._get_overlap_interval(st1, ed1, st2, ed2)

            if is_ov:  # 有交集
                st_ov, ed_ov = rng
                if st_ov == st1 and ed_ov == ed1:  # 如果取交集範圍 == api 回傳資料範圍則無須切片
                    pass
                else:
                    left_idx = bisect_left(raw.ts, st_ov)
                    right_idx = bisect_right(raw.ts, ed_ov)
                    self._slice_ticks_inplace(raw, left_idx, right_idx)
                np_data = self._data_spec.np_data_type.from_raw(raw, self._data_spec.field_enum)
            else:
                np_data = None
        else:
            np_data = None

        single = (start, np_data)

        data_queue.put(single)
        return single

    def _to_db(self, conn: Connection, symbol: str, data_queue: Queue):
        while True:
            item: None | tuple = data_queue.get()
            if item is None:
                data_queue.task_done()
                break

            dt: date
            np_data: D
            dt, np_data = item

            with conn.cursor() as cur:
                with cur.copy(self.copy_tick_sql) as copy:
                    if np_data:
                        copy.write(_ApiFetcherBase.COPY_BINARY_HEADER)
                        npy_bytes = npy_pack(symbol, np_data, self._data_spec.field_enum)
                        copy.write(npy_bytes)
                        copy.write(_ApiFetcherBase.COPY_BINARY_TRAILER)
                        self._logger.info(f'({symbol}, {dt.strftime('%Y-%m-%d')}): {len(np_data.ts)} items.')
                        self._logger.info(
                            f'range: {pg_us_to_datetime(np_data.ts[0])} to {pg_us_to_datetime(np_data.ts[-1])}'
                        )
                    else:
                        self._logger.info('no tick has been written.')

                cur.execute(self.insert_memo_sql, (dt, symbol))

            conn.commit()
            data_queue.task_done()

    @property
    def copy_tick_sql(self):
        return self._COPY_TICK_SQL_TEMPL.format(
            self._data_spec.table_name,
            ', '.join(self._data_spec.field_enum.names())
        )

    @property
    def insert_memo_sql(self):
        return self._INSERT_MEMO_SQL_TEMPL.format(self._data_spec.table_name)

    @abstractmethod
    def _fetch_from_api(self, contract, start):
        ...
