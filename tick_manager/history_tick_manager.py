import traceback
from datetime import timedelta, time

from shioaji.constant import TicksQueryType
from shioaji.data import Ticks
from sqlalchemy.orm import Session, sessionmaker
from typing_extensions import override

from database.schema.history_tick import HistoryTickMemo, HistoryTick
from tick_manager.history_data_manager_base import HistoryDataManagerBase
from tools.constants import EXP86400
from tools.utils import history_ts_to_datetime, replace_time, is_in_time_ranges


class HistoryTickManager(HistoryDataManagerBase[HistoryTick]):

    def __init__(self, api, redis, session_maker: sessionmaker[Session], log_on=True):
        super().__init__(api, redis, session_maker, log_on)

    @property
    def _redis_key_prefix(self):
        return 'history.tick'

    def _get_data_from_db(self, symbol, start):
        with self.session_maker() as session:
            memo = session.query(HistoryTickMemo).get(
                {"date": self._dt(start).date(), "symbol": symbol}
            )
            if memo:
                # 將字串轉換為datetime對象
                date_obj = self._dt(start)

                previous_day = date_obj - timedelta(days=1)

                start_time = previous_day.replace(hour=15, minute=0, second=0)
                end_time = date_obj.replace(hour=13, minute=45, second=0)
                results = session.query(HistoryTick).filter(
                    HistoryTick.ts.between(start_time, end_time),
                    HistoryTick.symbol == symbol  # noqa
                ).order_by(HistoryTick.ts).all()

                return True, results
            return False, []

    def _set_data_to_db(self, data, symbol, start):
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
            date=self._dt(start).date(),
            symbol=symbol
        )

        suc = self._commit_to_db(HistoryTick.__tablename__, [start], new_ticks, [memo])
        return (suc, new_ticks) if suc else (suc, [])

    def _get_data_from_redis(self, symbol, start: str):
        key = self._redis_key(symbol, start)

        if self.redis.exists(self._memo_key(key)):
            # if not ranges:
                results = self.redis.zrange(key, 0, -1)
                return True, [HistoryTick.from_string(ht) for ht in results]
            # else:
            #     timestamp_ranges = self._translate_time_ranges(start, ranges)
            #     pipe = self.redis.pipeline()
            #     for l, r in timestamp_ranges:
            #         pipe.zrangebyscore(key, l, r)
            #     results = pipe.execute(raise_on_error=True)
                return True, [HistoryTick.from_string(ht) for ranged_ht in results for ht in ranged_ht]
        return False, []

    def _set_data_to_redis(self, data: list[HistoryTick], symbol, start):
        pipe = self.redis.pipeline()
        key = self._redis_key(symbol, start)
        if data:
            redis_data = {
                d.to_string(): d.ts.timestamp() for d in data
            }

            pipe.zadd(key, redis_data)
            pipe.expire(key, EXP86400)
        else:
            self._log('no history tick data.(might be weekends?)')

        pipe.set(self._memo_key(key), self.redis_memo_default_value, ex=EXP86400)

        return pipe.execute(True)

    def _get_data_from_api(self, contract, start) -> Ticks:
        ticks = self.api.ticks(
            contract,
            start,
            TicksQueryType.AllDay,
        )
        return ticks

    def _prepare_data(self, contract, start):
        symbol = contract.symbol

        self._log('Fetching..')
        ticks = self._get_data_from_api(contract, start)

        self._log('Set ticks to db..')
        suc, history_ticks = self._set_data_to_db(ticks, symbol, start)

        if not suc:
            return False, []

        self._log('Set ticks to redis..')
        suc = self._set_data_to_redis(history_ticks, symbol, start)

        if not suc:
            return False, []

        return True, history_ticks

    # def _translate_time_ranges(self, start, time_ranges) -> list[tuple[float, float]]:
    #     timestamp_ranges = []
    #     for l, r in time_ranges:
    #         timestamp_ranges.append(
    #             (
    #                 replace_time(self._dt(start), l).timestamp(),
    #                 replace_time(self._dt(start), r).timestamp()
    #             )
    #         )
    #     return timestamp_ranges

    def _get_data(self, contract, start: str)-> list[HistoryTick]:
        self._date_check(start)
        symbol = contract.symbol

        self._log(f'Try load {self._type_name} from redis...')
        suc, data = self._get_data_from_redis(symbol, start)
        if suc:
            return data

        self._log(f'Try load {self._type_name} from db...')
        suc, data = self._get_data_from_db(symbol, start)
        if suc:
            self._log('DB -> redis...')
            self._set_data_to_redis(data, symbol, start)
            return data

        # fetch new data
        self._log(f'Fetch {self._type_name} from api...')
        suc, data = self._prepare_data(contract, start)
        if not suc:
            return []

        return data

    def get_data(self, contract, start: str, time_ranges: list[tuple[time, time]] = None) -> list[HistoryTick]:
        data = self._get_data(contract,start)
        if time_ranges:
            filtered = []
            for t in data:
                if is_in_time_ranges(t.ts.time(),time_ranges):
                    filtered.append(t)
            return filtered
        return data
