from datetime import date, time

from psycopg import Connection
from sqlalchemy import select

from data_manager.history.statics.memo_protocols import MemoProtocol
from tools.utils import get_now


def get_missing_dates(conn: Connection, symbol: str, table_name: str, dates: set[date]) -> set[date]:
    """查詢 DB 中已存在的日期，回傳缺失的日期集合。"""
    if not dates:
        return set()
    with conn.cursor() as cur:
        sql = "SELECT date FROM {0} WHERE symbol = %s AND date = ANY(%s)".format(table_name)
        cur.execute(sql, (symbol, list(dates)))
        existing = {row[0] for row in cur}
    return dates - existing


def range_check(start: date, end: date = None):
    """
    確認時間範圍是否在容許條件內:
    1. end >= start
    2. 現在時間 >= end 日 的 全日收盤時間(13:45:00)
    :param start:
    :param end:
    :return:
    """

    if end:
        if start > end:
            raise Exception("start cannot greater than end!")
        chk_date_dt = end
    else:
        chk_date_dt = start

    now = get_now()

    over_today = chk_date_dt > now.date()
    is_today = chk_date_dt == now.date()
    currently_not_closed = now.time() < time(13, 45, 5)

    if over_today or (is_today and currently_not_closed):
        raise Exception(f"data is not completed yet.")
