from datetime import date
from functools import lru_cache
from typing import Iterable

from dateutil.relativedelta import relativedelta

_CREATE_PARTITION_STMT_TEMPLATE_2 = "CREATE TABLE IF NOT EXISTS {0} PARTITION OF {1} FOR VALUES FROM ('{2}') TO ('{3}')"


@lru_cache(maxsize=None)
def _partition_name(table_name, dt: date):
    return f'{table_name}_{dt.strftime("%Y%m")}'


def create_partition_table_2(session, table_name, dates: Iterable[date]):
    firsts = set()  # 放每月一號

    for d in dates:
        firsts.add(d.replace(day=1))
        if d.day == 1:
            firsts.add(d - relativedelta(months=1))

    for fst in firsts:
        session.execute(
            _CREATE_PARTITION_STMT_TEMPLATE_2.format(
                _partition_name(table_name, fst),
                table_name,
                fst,
                fst + relativedelta(months=1)
            )
        )
