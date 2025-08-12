import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypeVar, Generic, get_args

from redis import Redis
from shioaji import Shioaji
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from tools.constants import DATE_FORMAT_SHIOAJI
from tools.utils import get_now

T = TypeVar("T")


class AbsHistoryDataManager(ABC, Generic[T]):
    data_type: type
    date_format_db = '%Y-%m-%d'
    date_format_redis = '%Y.%m.%d'

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

    @abstractmethod
    def _get_data_from_db(self, symbol, start, end=None):
        pass

    @abstractmethod
    def _set_data_to_db(self, data, symbol, start, end=None):
        pass

    @abstractmethod
    def _get_data_from_redis(self, symbol, start, end=None):
        pass

    @abstractmethod
    def _set_data_to_redis(self, data: list[T], symbol, start, end=None):
        pass

    @abstractmethod
    def _get_data_from_api(self, contract, start, end=None):
        pass

    @abstractmethod
    def _prepare_data(self, contract, start, end=None):
        pass

    def _create_partition_table(self, session: Session, date_str: str, table_name: str):
        date = datetime.strptime(date_str, self.date_format_db)

        partition_name = f'{table_name}_{date.strftime("%Y%m")}'

        start_date = date.replace(day=1)  # 当前月份的第一天

        year = date.year
        month = date.month

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
                        '{start_date.strftime(self.date_format_db)}', 
                        '{end_date.strftime(self.date_format_db)}'
                    );
                END IF;
            END $$;
            """
        session.execute(text(sql))

    def _commit_to_db(self, table_name, date, data, memo):
        with self.session_maker() as session:
            try:
                self._create_partition_table(session, date, table_name)
                session.add_all(data)
                session.add(memo)
                session.commit()

            except Exception as e:
                # 捕獲並處理例外
                print(f"發生錯誤: {traceback.format_exc()}")
                session.rollback()
                return False
        return True

    def _date_check(self, start: str, end: str = None):
        start_dt = datetime.strptime(start, DATE_FORMAT_SHIOAJI)

        if end:
            end_dt = datetime.strptime(end, DATE_FORMAT_SHIOAJI)
            if start_dt > end_dt:
                raise Exception("start cannot greater than end!")
            chk_date_dt = end_dt
        else:
            chk_date_dt = start_dt

        now = get_now()
        if not (
                chk_date_dt.date() < now.date() or
                (
                        # now == 要求日期且早盤已收
                        chk_date_dt.date() == now.date()
                        and
                        (
                                (now.hour == 13 and now.minute >= 45)
                                or
                                now.hour >= 14
                        )
                )
        ):
            raise Exception(f"{self._type_name} is not complete yet.")

    def get_data(self, contract, start: str, end: str = None) -> list[T]:
        self._date_check(start, end)

        symbol = contract.symbol

        self._log(f'Try load {self._type_name} from redis...')
        suc, data = self._get_data_from_redis(symbol, start, end)
        if suc:
            return data

        self._log(f'Try load {self._type_name} from db...')
        suc, data = self._get_data_from_db(symbol, start, end)
        if suc:
            self._log('DB -> redis...')
            self._set_data_to_redis(data, symbol, start, end)
            return data

        # fetch new data
        self._log(f'Fetch {self._type_name} from api...')
        suc, data = self._prepare_data(contract, start, end)
        if not suc:
            return []

        return data

    def _log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)
