import traceback
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, date
from functools import lru_cache
from typing import get_args, Type, Protocol

from redis import Redis
from shioaji import Shioaji
from sqlalchemy import text, select, Column
from sqlalchemy.orm import Session, sessionmaker

from tools.constants import DATE_FORMAT_DB_AND_SJ, DATE_FORMAT_REDIS
from tools.date_range_utils import enumerate_dates_set_by_range
from tools.utils import get_now


class HistoryDataProtocol(Protocol):
    symbol: Column
    ts: Column | datetime
    close: Column


class MemoProtocol(Protocol):
    date: Column
    symbol: Column


class HistoryDataManagerBase[THistoryData:HistoryDataProtocol, TMemo:MemoProtocol](ABC):
    data_type: Type[THistoryData]
    memo_type: Type[TMemo]
    redis_memo_default_value = 0

    def __init__(self, api, redis, session_maker, log_on=True):
        self.api: Shioaji = api
        self.redis: Redis = redis
        self.session_maker: sessionmaker[Session] = session_maker
        self.log_on = log_on
        self._tpe = ThreadPoolExecutor(max_workers=8)

    def __init_subclass__(cls):
        # cls.__orig_bases__ 會記錄泛型參數
        for base in cls.__orig_bases__:
            args = get_args(base)
            if args:
                cls.data_type = args[0]
                cls.memo_type = args[1]
                break

    @property
    def _type_name(self) -> str:
        return self.data_type.__name__

    @property
    @abstractmethod
    def _redis_key_prefix(self):
        pass

    def _redis_key(self, symbol, dt: str | date | datetime):
        if isinstance(dt, (date, datetime)):
            dt = dt.strftime(DATE_FORMAT_REDIS)
        return f'{self._redis_key_prefix}:{symbol}:{dt}'

    @staticmethod
    def _memo_key(redis_key):
        return f'memo.{redis_key}'

    def _create_partition_table(self, session: Session, _date: str | date, table_name: str):
        if isinstance(_date, str):
            _date = self._dt(_date)

        partition_name = f'{table_name}_{_date.strftime("%Y%m")}'

        start_date: date = _date.replace(day=1)  # 当前月份的第一天

        year = _date.year
        month = _date.month

        if month < 12:
            end_date = start_date.replace(month=month + 1)  # 下个月的第一天
        else:
            end_date = start_date.replace(year=year + 1, month=1)

        sql = f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = '{partition_name}') THEN
                    EXECUTE format('
                        CREATE TABLE %I PARTITION OF {table_name} FOR VALUES FROM (%L) TO (%L)',
                        '{partition_name}', 
                        '{self._dt_str(start_date)}', 
                        '{self._dt_str(end_date)}'
                    );
                END IF;
            END $$;
            """
        session.execute(text(sql))

    def _commit_to_db_with_session(self, session, table_name, dates: list, data: list, memos: list):
        try:
            for _date in dates:
                self._create_partition_table(session, _date, table_name)
            session.add_all(data)
            session.add_all(memos)
            session.commit()

        except Exception:
            # 捕獲並處理例外
            print(f"發生錯誤: {traceback.format_exc()}")
            session.rollback()
            raise

    def _commit_to_db(self, table_name, dates: list, data: list, memos: list):
        with self.session_maker() as session:
            self._commit_to_db_with_session(session, table_name, dates, data, memos)

    def _date_check(self, start: str | date, end: str | date = None):
        start = self._dt(start).date() if isinstance(start, str) else start

        if end:
            end = self._dt(end).date() if isinstance(end, str) else end
            if start > end:
                raise Exception("start cannot greater than end!")
            chk_date_dt = end
        else:
            chk_date_dt = start

        now = get_now()
        if not (
                chk_date_dt < now.date() or
                (
                        # now == 要求日期且早盤已收
                        chk_date_dt == now.date()
                        and
                        (
                                (now.hour == 13 and now.minute >= 45)
                                or
                                now.hour >= 14
                        )
                )
        ):
            raise Exception(f"{self._type_name} is not complete yet.")

    @staticmethod
    @lru_cache(maxsize=None)
    def _dt(date_str, fmt=DATE_FORMAT_DB_AND_SJ):
        return datetime.strptime(date_str, fmt)

    @staticmethod
    @lru_cache(maxsize=None)
    def _dt_str(dt: date, fmt=DATE_FORMAT_DB_AND_SJ):
        return dt.strftime(fmt)

    def _log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)

    @staticmethod
    @lru_cache(maxsize=128)
    def _get_existing_date_memos_by_range_stmt(memo_type: Type[TMemo], symbol, start: date, end: date):
        stmt = select(memo_type.date).where(
            memo_type.symbol == symbol,
            memo_type.date.between(start, end),
        )
        return stmt

    @staticmethod
    @lru_cache(maxsize=128)
    def _get_existing_date_memos_by_dates_stmt(memo_type: Type[TMemo], symbol, dates: list[date]):
        stmt = select(memo_type.date).where(
            memo_type.symbol == symbol,
            memo_type.date.in_(dates),
        )
        return stmt

    def _get_exising_dates_memo_by_dates(self, session, symbol, dates: list[date]):
        stmt = self._get_existing_date_memos_by_dates_stmt(self.memo_type, symbol, dates)
        memo_dates = session.execute(stmt).scalars().all()
        return memo_dates

    def _get_missing_dates(
            self,
            session,
            symbol,
            start: date = None,
            end: date = None,
            dates: list[date] = None
    ) -> list[date]:
        if start and end:
            stmt = self._get_existing_date_memos_by_range_stmt(self.memo_type, symbol, start, end)
            all_dates_set = enumerate_dates_set_by_range(start, end)
        elif dates:
            stmt = self._get_existing_date_memos_by_dates_stmt(self.memo_type, symbol, dates)
            all_dates_set = set(dates)
        else:
            raise Exception('no range or dates given.')

        memo_dates = session.execute(stmt).scalars().all()
        existing_dates_set = set(memo_dates)

        missing_dates = list(all_dates_set - existing_dates_set)
        missing_dates.sort()
        return missing_dates
