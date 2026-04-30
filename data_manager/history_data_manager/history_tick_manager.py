import heapq
from dataclasses import dataclass, field
from datetime import timedelta, time, date, datetime
from functools import lru_cache

from shioaji.constant import TicksQueryType
from shioaji.data import Ticks
from sqlalchemy import select, union_all, literal
from sqlalchemy.orm import Session, sessionmaker, aliased

from data_manager.history_data_manager.history_data_manager_base import HistoryDataManagerBase
from database.schema.history_tick import HistoryTickMemo, HistoryTick
from tools.constants import EXP86400
from tools.utils import history_ts_to_datetime, is_in_time_ranges


@dataclass
class DailyTicks:
    date: date = None
    ticks: list[HistoryTick] = field(default_factory=list)


class HistoryTickManager(HistoryDataManagerBase[HistoryTick, HistoryTickMemo]):

    def __init__(self, api, redis, session_maker: sessionmaker[Session], log_on=True):
        super().__init__(api, redis, session_maker, log_on)

    @property
    def _redis_key_prefix(self):
        return 'history.tick'

    def _get_memos_exists_by_dates(self, session, symbol: str, dates: list[date]):
        stmt = select(HistoryTickMemo.date).where(
            HistoryTickMemo.symbol == symbol,
            HistoryTickMemo.date.in_(dates),
        )
        return self._get_existing_date_memos(session, stmt)

    def _get_data_from_api(self, contract, start: str) -> Ticks:
        ticks = self.api.ticks(
            contract,
            start,
            TicksQueryType.AllDay,
        )
        return ticks

    def _fetch_data_to_db_and_return_it(self, session, contract, fetch_dates: list[date]) -> list[DailyTicks]:
        tasks = [
            self._tpe.submit(
                self._get_data_from_api,
                contract,
                self._dt_str(_start)
            ) for _start in fetch_dates
        ]
        api_data: list[Ticks] = [task.result() for task in tasks]

        db_data = []
        for day_data, dt in zip(api_data, fetch_dates):
            _, db_ticks = self._set_data_to_db(session, day_data, contract.symbol, dt)
            db_data.append(DailyTicks(dt, db_ticks))
        return db_data

    def _get_data_from_db(self, session, symbol, start: date) -> tuple[bool, DailyTicks]:
        has_memo = self._get_memos_exists_by_dates(
            session,
            symbol,
            [start]
        )[0]

        if has_memo:
            stmt = self._get_data_from_db_stmt(symbol, start)
            results = session.execute(stmt).scalars().all()

            return True, DailyTicks(start, results)
        return False, DailyTicks()

    @staticmethod
    @lru_cache(maxsize=128)
    def _get_data_from_db_stmt(symbol, start: date, qid=None):
        previous_day = start - timedelta(days=1)

        s = datetime.combine(previous_day, time(hour=15, minute=0, second=0))
        e = datetime.combine(start, time(hour=13, minute=45, second=0))

        if qid:
            col = (HistoryTick, literal(qid).label("qid"))
        else:
            col = (HistoryTick,)

        stmt = select(*col).where(
            HistoryTick.ts.between(s, e),
            HistoryTick.symbol == symbol
        ).order_by(HistoryTick.ts)
        return stmt

    def _get_data_from_db_batch(self, session, symbol, dates: list[date]) -> list[DailyTicks]:
        stmts = []
        for dt in dates:
            stmt = self._get_data_from_db_stmt(symbol, dt, dt)
            stmts.append(stmt)

        query = union_all(*stmts).subquery()
        tick_alias = aliased(HistoryTick, query)

        raw_results = session.query(tick_alias, query.c.qid).all()

        organized_dict = {d: [] for d in dates}
        for tick, q_date in raw_results:
            organized_dict[q_date].append(tick)
        return sorted(
            [DailyTicks(i[0], i[1]) for i in organized_dict.items()],
            key=lambda x: x.date
        )

    def _set_data_to_db(self, session, data: Ticks, symbol, start: date):
        ticks_len = len(data.ts)

        new_ticks = []
        for i in range(ticks_len):
            new_ticks.append(HistoryTick(
                ts=history_ts_to_datetime(data.ts[i]),
                symbol=symbol,
                close=data.close[i],
                volume=data.volume[i],
                bid_price=data.bid_price[i],
                bid_volume=data.bid_volume[i],
                ask_price=data.ask_price[i],
                ask_volume=data.ask_volume[i],
                tick_type=data.tick_type[i],
            ))
        memo = HistoryTickMemo(
            date=start,
            symbol=symbol
        )

        suc = self._commit_to_db_with_session(session, HistoryTick.__tablename__, [start], new_ticks, [memo])
        return (suc, new_ticks) if suc else (suc, [])

    def _get_data_from_redis(self, symbol, dt: date) -> tuple[bool, DailyTicks]:
        key = self._redis_key(symbol, dt)

        if self.redis.exists(self._memo_key(key)):
            # if not ranges:
            results = self.redis.zrange(key, 0, -1)
            data = DailyTicks(
                dt,
                [HistoryTick.from_string(ht) for ht in results]
            )
            return True, data

        return False, DailyTicks()

    def _get_missing_dates_and_existing_data_in_redis(
            self,
            symbol,
            check_dates: list[date]
    ) -> tuple[list[date], list[DailyTicks]]:

        pipe = self.redis.pipeline()
        redis_keys = []

        for d in check_dates:
            redis_key = self._redis_key(symbol, d)
            redis_keys.append(redis_key)
            memo_key = self._memo_key(redis_key)
            pipe.exists(memo_key)

        exists_bit = pipe.execute()
        existing_dates = []
        existing_keys = []
        missing_dates = []

        for e, d, k in zip(exists_bit, check_dates, redis_keys):
            if e:
                existing_dates.append(d)
                existing_keys.append(k)
            else:
                missing_dates.append(d)

        # get existing data
        for k in existing_keys:
            pipe.zrange(k, 0, -1)
        redis_tick_raw = pipe.execute()
        redis_ticks = []
        for dt, raw_ticks in zip(existing_dates, redis_tick_raw):
            redis_ticks.append(
                DailyTicks(
                    dt,
                    [HistoryTick.from_string(raw_tick) for raw_tick in raw_ticks]
                )
            )

        return missing_dates, redis_ticks

    def _set_data_to_redis(self, symbol, data: list[DailyTicks]):
        if not data:
            return [1]

        pipe = self.redis.pipeline()

        for daily_ticks in data:
            key = self._redis_key(symbol, daily_ticks.date)
            if daily_ticks.ticks:
                redis_data = {tick.to_string(): tick.ts.timestamp() for tick in daily_ticks.ticks}
                pipe.zadd(key, redis_data)
                pipe.expire(key, EXP86400)

            else:
                self._log('no history tick data.(might be weekends?)')

            pipe.set(self._memo_key(key), self.redis_memo_default_value, ex=EXP86400)

        return pipe.execute(True)

    def _prepare_data(self, session, contract, start: date) -> tuple[bool, DailyTicks]:
        symbol = contract.symbol

        self._log('Fetching..')
        ticks = self._get_data_from_api(contract, self._dt_str(start))

        self._log('Set ticks to db..')
        suc, history_ticks = self._set_data_to_db(session, ticks, symbol, start)

        if not suc:
            return False, DailyTicks()

        self._log('Set ticks to redis..')
        data = DailyTicks(start, history_ticks)
        suc = self._set_data_to_redis(symbol, [data])

        if not suc:
            return False, DailyTicks()

        return True, data

    def _get_data(self, contract, dt: date) -> list[HistoryTick]:
        self._date_check(dt)
        symbol = contract.symbol

        self._log(f'Try load {self._type_name} from redis...')
        suc, data = self._get_data_from_redis(symbol, dt)
        if suc:
            return data.ticks

        self._log(f'Try load {self._type_name} from db...')

        with self.session_maker() as session:
            suc, data = self._get_data_from_db(session, symbol, dt)
            if suc:
                self._log('DB -> redis...')
                self._set_data_to_redis(symbol, [data])
                return data.ticks

            # fetch new data
            self._log(f'Fetch {self._type_name} from api...')
            suc, data = self._prepare_data(session, contract, dt)
            if not suc:
                return []
            return data.ticks

    def get_data(self, contract, str_date: str, time_ranges: list[tuple[time, time]] = None) -> list[HistoryTick]:
        dt = self._dt(str_date).date()
        data = self._get_data(contract, dt)
        if time_ranges:
            filtered = []
            for t in data:
                if is_in_time_ranges(t.ts.time(), time_ranges):
                    filtered.append(t)
            return filtered
        return data

    def get_data_batch(self, contract, str_start: str, str_end: str) -> list[DailyTicks]:
        self._date_check(str_start, str_end)
        symbol = contract.symbol

        # type check (if needed?)
        start = self._dt(str_start).date()
        end = self._dt(str_end).date()
        enum_all_dt: set[date] = self._enumerate_dates_by_range(start, end)

        api_data = []
        db_data = []
        redis_data = []

        # check db missing (fetch if missing)
        with self.session_maker() as session:
            list_db_missing_dt: list[date] = self._get_missing_dates_by_range(session, symbol, start, end)
            if list_db_missing_dt:
                api_data = self._fetch_data_to_db_and_return_it(session, contract, list_db_missing_dt)
                assert len(list_db_missing_dt) == len(api_data)

            # check redis missing (query db if missing)
            list_redis_check_dates: list[date] = list(enum_all_dt - set(list_db_missing_dt))
            list_redis_check_dates.sort()
            list_redis_missing, redis_data = self._get_missing_dates_and_existing_data_in_redis(
                symbol,
                list_redis_check_dates
            )
            if list_redis_missing:
                db_data = self._get_data_from_db_batch(session, symbol, list_redis_missing)

        # set all missing data to redis
        if api_data:
            self._set_data_to_redis(symbol, api_data)
        if db_data:
            self._set_data_to_redis(symbol, db_data)

        # sort, combine, return
        return list(
            heapq.merge(
                api_data, db_data, redis_data,
                key=lambda x: x.date
            )
        )
