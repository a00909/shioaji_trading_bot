from abc import ABC
from datetime import date

from psycopg import Connection

from data_manager.history.common._npy_binary_packer import npy_unpack
from data_manager.history.statics.base._history_data_spec import _HistoryDataSpec
from data_manager.history.statics.base._np_data_base import _NpDataBase
from tools.logger.custom_logger import CustomLogger


class _DbLoaderBase[D:_NpDataBase](ABC):
    def __init__(self, data_spec: _HistoryDataSpec):
        self._logger = CustomLogger.get_logger(f'{data_spec.logger_prefix}_db_loader')
        self._data_spec = data_spec

    def copy_stmt(self, symbol, dt):
        l, r = self._data_spec.daily_time_range(dt)
        l = l.strftime("%Y-%m-%d %H:%M:%S")
        r = r.strftime("%Y-%m-%d %H:%M:%S")
        return f"""
            COPY (
                SELECT {', '.join(self._data_spec.field_enum.names())} 
                FROM {self._data_spec.table_name}
                WHERE symbol='{symbol}' and ts between '{l}' and '{r}'
                ORDER BY ts ASC
            )TO STDIN (FORMAT BINARY)
        """

    def load(self, conn: Connection, symbol, dates: set[date]) -> dict[date, D]:
        data = {}

        if not dates:
            return data

        with conn.cursor() as cur:
            for dt in dates:
                sql = self.copy_stmt(symbol, dt)
                with cur.copy(sql) as copy:
                    chunks = []
                    while chunk := copy.read():
                        chunks.append(chunk)
                    raw = b''.join(chunks)
                    self._logger.info(f'{dt.strftime("%Y-%m-%d")}: {len(chunks)} chucks read. {len(raw)} bytes.')
                    np_data = npy_unpack(self._data_spec.field_enum, self._data_spec.np_data_type, raw[19:-2])
                    data[dt] = np_data
        return data
