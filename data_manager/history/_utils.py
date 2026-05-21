from datetime import date, time

from sqlalchemy import select

from data_manager.history.statics.memo_protocols import MemoProtocol
from tools.utils import get_now


def get_missing_dates(session, symbol, memo_type: type[MemoProtocol], dates: set[date]):
    # todo: 這裡改用psycopg
    stmt = select(memo_type.date).where(
        memo_type.symbol == symbol,
        memo_type.date.in_(dates),
    )
    memo_dates = session.execute(stmt).scalars().all()
    existing_dates_set = set(memo_dates)

    missing_dates = dates - existing_dates_set
    return missing_dates


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
