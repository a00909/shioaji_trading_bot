import traceback
from abc import ABC, abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, date, timedelta
from functools import lru_cache
from typing import get_args, Type, Protocol

from pydantic import deprecated
from redis import Redis
from shioaji import Shioaji
from sqlalchemy import text, select, Column
from sqlalchemy.orm import Session, sessionmaker

from tools.constants import DATE_FORMAT_DB_AND_SJ, DATE_FORMAT_REDIS
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

        except Exception as e:
            # 捕獲並處理例外
            print(f"發生錯誤: {traceback.format_exc()}")
            session.rollback()
            return False
        return True

    
    def _commit_to_db(self, table_name, dates: list, data: list, memos: list):
        with self.session_maker() as session:
            return self._commit_to_db_with_session(session, table_name, dates, data, memos)

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
    @lru_cache(maxsize=128)
    def _dt(date_str, fmt=DATE_FORMAT_DB_AND_SJ):
        return datetime.strptime(date_str, fmt)

    @staticmethod
    @lru_cache(maxsize=128)
    def _dt_str(dt: date, fmt=DATE_FORMAT_DB_AND_SJ):
        return dt.strftime(fmt)

    def _log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)

    @staticmethod
    def _group_dates_into_ranges(dates: list[date]) -> list[tuple[date, date]]:
        if not dates:
            return []

        sorted_dates = sorted(dates)
        ranges = []
        range_start = sorted_dates[0]
        prev_date = sorted_dates[0]

        for current in sorted_dates[1:]:
            if current == prev_date + timedelta(days=1):
                prev_date = current
            else:
                ranges.append((range_start, prev_date))
                range_start = current
                prev_date = current

        ranges.append((range_start, prev_date))
        return ranges

    @staticmethod
    def _subtract_ranges(bigger: list[tuple[date, date]], smaller: list[tuple[date, date]]) -> list[tuple[date, date]]:
        """
        Compute A - B where A and B are sorted, non-overlapping ranges (inclusive).
        Robust: handles B ranges that may span across multiple A ranges or lie outside A.
        Returns a list of (start_date, end_date) tuples (inclusive).
        Time complexity: O(len(A) + len(B)).
        """
        result = []
        # make a mutable copy of B so we can update start when B spans multiple A's
        b_list = [[b_start, b_end] for (b_start, b_end) in smaller]
        j = 0

        for a_start, a_end in bigger:
            cur_start = a_start

            # skip B's that end before current A's start
            while j < len(b_list) and b_list[j][1] < cur_start:
                j += 1

            while j < len(b_list):
                b_start, b_end = b_list[j]

                # if the next B starts after current A ends, done with this A
                if b_start > a_end:
                    break

                # keep the gap before B within current A
                if cur_start < b_start:
                    result.append((cur_start, b_start - timedelta(days=1)))

                # consume the intersection of B with current A
                if b_end <= a_end:
                    # B finishes inside current A -> move cur_start after B and consume B entirely
                    cur_start = b_end + timedelta(days=1)
                    j += 1
                else:
                    # B extends beyond current A -> update B's start to the first day after current A
                    # (so the remaining part of B will be applied to subsequent A segments)
                    b_list[j][0] = a_end + timedelta(days=1)
                    cur_start = b_end + timedelta(days=1)  # this will be > a_end, so loop exits
                    break

            # leftover tail of A
            if cur_start <= a_end:
                result.append((cur_start, a_end))

        return result

    @staticmethod
    def _get_existing_date_memos(session, stmt):
        memo_dates = session.execute(stmt).scalars().all()
        existing_set = set(memo_dates)
        return existing_set

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
    def _enumerate_dates_by_range(start: date, end: date):
        # 使用 frozenset 確保快取結果不可被外部修改
        all_dates = frozenset(
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
        )
        return all_dates

    def _get_missing_dates_by_range(self, session, symbol, start, end):
        stmt = self._get_existing_date_memos_by_range_stmt(self.memo_type, symbol, start, end)
        existing_dates = self._get_existing_date_memos(session, stmt)

        all_dates = self._enumerate_dates_by_range(start, end)

        missing_dates = list(all_dates - existing_dates)
        missing_dates.sort()
        return missing_dates

    def _find_missing_ranges_db(self, session, symbol, start: date, end: date):
        # 找出缺漏
        missing_dates = self._get_missing_dates_by_range(session, symbol, start, end)
        date_ranges = self._group_dates_into_ranges(missing_dates)
        self._log(
            f'All ranges: {[(self._dt_str(s), self._dt_str(t)) for s, t in date_ranges]}'
        )

        return date_ranges
