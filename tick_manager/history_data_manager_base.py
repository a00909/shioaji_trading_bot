import traceback
from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import TypeVar, Generic, get_args

from redis import Redis
from shioaji import Shioaji
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from tools.constants import DATE_FORMAT_DB_AND_SJ, DATE_FORMAT_REDIS
from tools.utils import get_now

T = TypeVar("T")


class HistoryDataManagerBase(ABC, Generic[T]):
    data_type: type
    redis_memo_default_value = 0

    def __init__(self, api, redis, session_maker, log_on=True):
        self.api: Shioaji = api
        self.redis: Redis = redis
        self.session_maker: sessionmaker[Session] = session_maker
        self.log_on = log_on

    def __init_subclass__(cls):
        # cls.__orig_bases__ 會記錄泛型參數
        for base in cls.__orig_bases__:
            args = get_args(base)
            if args:
                cls.data_type = args[0]

    @property
    def _type_name(self) -> str:
        return self.data_type.__name__

    # @abstractmethod
    # def _get_data_from_db(self, symbol, start, end=None):
    #     pass
    #
    # @abstractmethod
    # def _set_data_to_db(self, data, symbol, start, end=None):
    #     pass
    #
    # @abstractmethod
    # def _get_data_from_redis(self, symbol, start, end=None):
    #     pass
    #
    # @abstractmethod
    # def _set_data_to_redis(self, data: list[T], symbol, start, end=None):
    #     pass
    #
    # @abstractmethod
    # def _get_data_from_api(self, contract, start, end=None):
    #     pass

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

        start_date = _date.replace(day=1)  # 当前月份的第一天

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
                        '{start_date.strftime(DATE_FORMAT_DB_AND_SJ)}', 
                        '{end_date.strftime(DATE_FORMAT_DB_AND_SJ)}'
                    );
                END IF;
            END $$;
            """
        session.execute(text(sql))

    def _commit_to_db(self, table_name, dates: list, data: list, memos: list):

        with self.session_maker() as session:
            try:
                for _date in dates:
                    self._create_partition_table(session, _date, table_name)
                session.add_all(data)
                session.add_all(memos)
                session.commit()

            except Exception as e:
                # 捕獲並處理例外
                print(f"發生錯誤: {traceback.format_exc()}")
                session.rollback()
                return False
        return True

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
    def _dt(date_str, fmt=DATE_FORMAT_DB_AND_SJ):
        return datetime.strptime(date_str, fmt)

    def _log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)
