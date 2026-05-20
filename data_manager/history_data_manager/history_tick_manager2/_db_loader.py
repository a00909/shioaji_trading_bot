import struct
from datetime import date, timedelta
from typing import cast

from psycopg import Connection
from shioaji.data import Ticks
from sqlalchemy import Engine

from data_manager.history_data_manager.history_tick_manager2._common import TICKS_FIELDS


class DBLoader:
    ticks_unpack_format = '> 2x 4xq 4xd 4xi 4xd 4xi 4xd 4xi 4xi'

    def __init__(self, engine):
        self._engine: Engine = engine

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

    def load(self, symbol, dates: set[date]) -> dict[date, Ticks]:
        data = {}
        with self._engine.raw_connection() as raw_conn:
            conn = cast(Connection, raw_conn.connection)
            with conn.cursor() as cur:
                for dt in dates:
                    sql = DBLoader.copy_stmt(symbol, dt)
                    with cur.copy(sql) as copy:
                        data_dict = {field: [] for field in TICKS_FIELDS}

                        raw = copy.read()[19:]  # 第一次過濾header
                        while raw:
                            if raw != b'\xff\xff':  # 結尾符
                                try:
                                    for row in struct.iter_unpack(DBLoader.ticks_unpack_format, raw):
                                        for field, value in zip(TICKS_FIELDS, row):
                                            data_dict[field].append(value)
                                except Exception as e:
                                    print(e, len(raw), [b for b in raw])
                            raw = copy.read()

                        ticks = Ticks(**data_dict)
                        data[dt] = ticks
        return data
