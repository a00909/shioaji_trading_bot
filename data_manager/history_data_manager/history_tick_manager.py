from dataclasses import dataclass, field
from datetime import timedelta, time, date, datetime
from functools import lru_cache
from typing import Any, cast

from shioaji.constant import TicksQueryType
from shioaji.contracts import Contract
from shioaji.data import Ticks
from sqlalchemy import select, union_all, literal
from sqlalchemy.orm import Session, sessionmaker, aliased

from data_manager.history_data_manager.history_data_manager_base import HistoryDataManagerBase
from database.schema.history_tick import HistoryTickMemo, HistoryTick
from tools.constants import EXP86400
from tools.date_range_utils import enumerate_dates_set_by_range
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

    def _get_data_from_api(self, contract: Contract, start: str) -> Ticks:
        ticks = self.api.ticks(
            contract,
            start,
            TicksQueryType.AllDay,
        )
        return ticks

    def _fetch_data_to_db_and_return_it(self, session, contract, fetch_dates: set[date]) -> dict[date, DailyTicks]:
        tasks = {
            _start:
                self._tpe.submit(
                    self._get_data_from_api,
                    contract,
                    self._dt_str(_start)
                ) for _start in fetch_dates
        }
        for dt, task in tasks.items():
            tasks[dt] = task.result()

        db_data = self._set_data_to_db_batch(session, contract.symbol, tasks)
        return db_data

    def _get_data_from_db(self, session, symbol, start: date) -> tuple[bool, DailyTicks]:
        existing: list = self._get_exising_dates_memo_by_dates(
            session,
            symbol,
            dates=[start]
        )

        if existing:
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

    def _get_data_from_db_batch(self, session, symbol, dates: set[date]) -> dict[date, DailyTicks]:
        stmts = []
        ordered_dts = []
        for dt in dates:
            stmt = self._get_data_from_db_stmt(symbol, dt, dt)
            stmts.append(stmt)
            ordered_dts.append(dt)

        query = union_all(*stmts).subquery()
        tick_alias = aliased(HistoryTick, query)

        raw_results = session.query(tick_alias, query.c.qid).all()

        organized_dict = {d: [] for d in ordered_dts}
        for tick, q_date in raw_results:
            organized_dict[q_date].append(tick)
        return {i[0]: DailyTicks(i[0], i[1]) for i in organized_dict.items()}

    def _set_data_to_db_batch(self, session, symbol, daily_data: dict[date, Ticks]):
        dt_to_tick_len = {dt: len(ticks.ts) for dt, ticks in daily_data.items()}
        num_ticks = sum(dt_to_tick_len.values())
        num_dates = len(daily_data)

        all_ticks: list[Any] = [None] * num_ticks
        memos: list[Any] = [None] * num_dates
        result: dict[date, Any] = {}

        all_ticks_count = 0
        for i, dt in enumerate(dt_to_tick_len):
            ticks_len = dt_to_tick_len[dt]
            day_ticks: list[Any] = [None] * ticks_len
            ticks = daily_data[dt]

            for j in range(ticks_len):
                new_tick = HistoryTick(
                    ts=history_ts_to_datetime(ticks.ts[j]),
                    symbol=symbol,
                    close=ticks.close[j],
                    volume=ticks.volume[j],
                    bid_price=ticks.bid_price[j],
                    bid_volume=ticks.bid_volume[j],
                    ask_price=ticks.ask_price[j],
                    ask_volume=ticks.ask_volume[j],
                    tick_type=ticks.tick_type[j],
                )
                day_ticks[j] = new_tick
                all_ticks[all_ticks_count] = new_tick
                all_ticks_count += 1

            result[dt] = DailyTicks(dt, day_ticks)

            memos[i] = HistoryTickMemo(
                date=dt,
                symbol=symbol
            )

        self._commit_to_db_with_session(session, HistoryTick.__tablename__, dt_to_tick_len.keys(), all_ticks, memos)
        return result

    def _set_data_to_db(self, session, data: Ticks, symbol, start: date):
        """
        session will be commited in the method
        :param session:
        :param data:
        :param symbol:
        :param start:
        :return:
        """
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

        self._commit_to_db_with_session(session, HistoryTick.__tablename__, [start], new_ticks, [memo])
        return new_ticks

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
            check_dates: set[date]
    ) -> tuple[set[date], dict[date, DailyTicks]]:

        pipe = self.redis.pipeline()
        redis_keys = []

        ordered_dates = []
        for d in check_dates:
            ordered_dates.append(d)
            redis_key = self._redis_key(symbol, d)
            redis_keys.append(redis_key)
            memo_key = self._memo_key(redis_key)
            pipe.exists(memo_key)

        exists_bit = pipe.execute()
        existing_dates = []
        existing_keys = []
        missing_dates = set()

        for e, d, k in zip(exists_bit, ordered_dates, redis_keys):
            if e:
                existing_dates.append(d)
                existing_keys.append(k)
            else:
                missing_dates.add(d)

        # get existing data
        for k in existing_keys:
            pipe.zrange(k, 0, -1)
        redis_tick_raw = pipe.execute()
        redis_ticks = {}
        for dt, raw_ticks in zip(existing_dates, redis_tick_raw):
            redis_ticks[dt] = DailyTicks(
                dt,
                [HistoryTick.from_string(raw_tick) for raw_tick in raw_ticks]
            )

        return missing_dates, redis_ticks

    def _set_data_to_redis(self, symbol, data: dict[date, DailyTicks]):
        if not data:
            raise Exception('no data to set to redis.')

        pipe = self.redis.pipeline()

        for daily_ticks in data.values():
            key = self._redis_key(symbol, daily_ticks.date)
            if daily_ticks.ticks:
                redis_data = {tick.to_string(): tick.ts.timestamp() for tick in daily_ticks.ticks}
                pipe.zadd(key, redis_data)
                pipe.expire(key, EXP86400)

            else:
                self._log('no history tick data.(might be weekends?)')

            pipe.set(self._memo_key(key), self.redis_memo_default_value, ex=EXP86400)

        pipe.execute(True)

    def _prepare_data(self, session, contract: Contract, start: date) -> tuple[bool, DailyTicks]:
        symbol = contract.symbol

        self._log('Fetching..')
        ticks = self._get_data_from_api(contract, self._dt_str(start))

        self._log('Set ticks to db..')
        history_ticks = self._set_data_to_db(session, ticks, symbol, start)

        self._log('Set ticks to redis..')
        data = DailyTicks(start, history_ticks)
        self._set_data_to_redis(symbol, [data])

        return True, data

    def _get_data(self, contract: Contract, dt: date) -> list[HistoryTick]:
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

    def get_data(self, contract: Contract, str_date: str, time_ranges: list[tuple[time, time]] = None) -> list[
        HistoryTick]:
        dt = self._dt(str_date).date()
        data = self._get_data(contract, dt)
        if time_ranges:
            filtered = []
            for t in data:
                if is_in_time_ranges(t.ts.time(), time_ranges):
                    filtered.append(t)
            return filtered
        return data

    def get_data_batch(
            self,
            contract: Contract,
            str_start: str = None,
            str_end: str = None,
            check_dates: set[date] = None
    ) -> list[DailyTicks]:

        if str_start and str_end:
            self._date_check(str_start, str_end)
            start = self._dt(str_start).date()
            end = self._dt(str_end).date()
            check_dates = enumerate_dates_set_by_range(start, end)

        elif check_dates is not None:
            if not check_dates:
                return []
        else:
            raise Exception('no range or dates given.')

        symbol = contract.symbol

        # type check (if needed?)

        api_data = []
        db_data = []
        redis_data = []

        # check db missing (fetch if missing)
        with self.session_maker() as session:
            db_missing_dts: set[date] = cast(
                set[date],
                self._get_missing_dates(session, symbol, dates=check_dates, return_set=True)
            )
            if db_missing_dts:
                api_data: dict[date, DailyTicks] = self._fetch_data_to_db_and_return_it(
                    session,
                    contract,
                    db_missing_dts
                )
                assert len(db_missing_dts) == len(api_data)

            # check redis missing (query db if missing)
            redis_check_dts: set[date] = check_dates - db_missing_dts
            redis_missing_dts, redis_data = self._get_missing_dates_and_existing_data_in_redis(
                symbol,
                redis_check_dts
            )
            if redis_missing_dts:
                db_data: dict[date, DailyTicks] = self._get_data_from_db_batch(session, symbol, redis_missing_dts)

        # set all missing data to redis
        if api_data:
            self._set_data_to_redis(symbol, api_data)
        if db_data:
            self._set_data_to_redis(symbol, db_data)

        # sort, combine, return
        result = []
        for dt in check_dates:
            if dt in api_data:
                result.append(api_data[dt])
            elif dt in db_data:
                result.append(db_data[dt])
            elif dt in redis_data:
                result.append(redis_data[dt])
            else:
                result.append([])

        return result
