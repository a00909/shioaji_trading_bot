from datetime import date, timedelta

from psycopg import Connection
from shioaji.data import Ticks

from data_manager.history.statics.tick_field import TICKS_FIELDS, UNPACK_STRUCT


class DBLoader:

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
    def load(conn: Connection, symbol, dates: set[date]) -> dict[date, Ticks]:
        data = {}

        if not dates:
            return data

        with conn.cursor() as cur:
            for dt in dates:
                sql = DBLoader.copy_stmt(symbol, dt)
                with cur.copy(sql) as copy:
                    data_dict = {field: [] for field in TICKS_FIELDS}

                    raw = copy.read()[19:]  # 第一次過濾header
                    while raw:
                        if raw != b'\xff\xff':  # 結尾符
                            try:
                                for row in UNPACK_STRUCT.unpack(raw):
                                    for field, value in zip(TICKS_FIELDS, row):
                                        data_dict[field].append(value)
                            except Exception as e:
                                print(e, len(raw), [b for b in raw])
                        raw = copy.read()

                    ticks = Ticks(**data_dict)
                    data[dt] = ticks

        return data
