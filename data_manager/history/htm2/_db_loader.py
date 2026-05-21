from datetime import date, timedelta

from psycopg import Connection
from shioaji.data import Ticks

from data_manager.history.htm2._npy_binary_packer import npy_unpack
from data_manager.history.statics.np_ticks import NPTicks
from data_manager.history.statics.tick_field import TICKS_FIELDS, UNPACK_STRUCT
from tools.logger.custom_logger import CustomLogger


class DBLoader:
    _logger = CustomLogger.get_logger('db_loader')

    @staticmethod
    def copy_stmt(symbol, dt):
        return f"""
            COPY (
                SELECT {', '.join(TICKS_FIELDS)} 
                FROM history_tick
                WHERE symbol='{symbol}' and ts between '{dt - timedelta(days=1)} 15:00:00' and '{dt} 13:45:05'
                ORDER BY ts ASC
            )TO STDIN (FORMAT BINARY)
        """

    @staticmethod
    def load(conn: Connection, symbol, dates: set[date]) -> dict[date, NPTicks]:
        data = {}

        if not dates:
            return data

        with conn.cursor() as cur:
            for dt in dates:
                sql = DBLoader.copy_stmt(symbol, dt)
                with cur.copy(sql) as copy:
                    chunks = []
                    while chunk := copy.read():
                        chunks.append(chunk)
                    raw = b''.join(chunks)
                    DBLoader._logger.info(f'{len(chunks)} chucks read. {len(raw)} bytes.')
                    ticks = npy_unpack(symbol, raw[19:-2])
                    data[dt] = ticks
        return data
