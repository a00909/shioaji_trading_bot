import traceback
from datetime import datetime, timedelta
from typing_extensions import override

from redis.client import Redis
from shioaji import Shioaji
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from database.schema.history_tick import HistoryTickMemo, HistoryTick
from tick_manager.abs_history_data_manager import AbsHistoryDataManager
from tools.utils import get_now, history_ts_to_datetime
from tools.constants import DATE_FORMAT_SHIOAJI


class HistoryTickManager(AbsHistoryDataManager[HistoryTick]):
    history_ticks_key_prefix = 'history.tick'

    def __init__(self, api, redis, session_maker: sessionmaker[Session], log_on=True):
        super().__init__(api, redis, session_maker, log_on)

    def redis_key(self, symbol, date):
        return f'{self.history_ticks_key_prefix}:{symbol}:{date}'

    @staticmethod
    def _memo_key(key):
        return f'{key}:memo'

    @override
    def _get_data_from_db(self, symbol, start, end=None):
        with self.session_maker() as session:
            memo = session.query(HistoryTickMemo).get(
                {"date": datetime.strptime(start, self.date_format_db).date(), "symbol": symbol}
            )
            if memo:
                # 將字串轉換為datetime對象
                date_obj = datetime.strptime(start, self.date_format_db)

                previous_day = date_obj - timedelta(days=1)

                start_time = previous_day.replace(hour=15, minute=0, second=0)
                end_time = date_obj.replace(hour=13, minute=45, second=0)
                results = session.query(HistoryTick).filter(
                    HistoryTick.ts.between(start_time, end_time),
                    HistoryTick.symbol == symbol
                ).order_by(HistoryTick.ts).all()

                return True, results
            return False, []

    @override
    def _set_data_to_db(self, data, symbol, start, end=None):
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
            date=datetime.strptime(start, self.date_format_db).date(),
            symbol=symbol
        )

        suc = self._commit_to_db(HistoryTick.__tablename__, start, new_ticks, memo)
        return suc, new_ticks if suc else suc, []

    @override
    def _get_data_from_redis(self, symbol, start, end=None):
        key = self.redis_key(symbol, start)

        if self.redis.sismember(self._memo_key(key), key):
            results = self.redis.lrange(key, 0, -1)
            return True, [HistoryTick.from_string(ht) for ht in results]
        return False, []

    @override
    def _set_data_to_redis(self, data: list[HistoryTick], symbol, start, end=None):
        pipe = self.redis.pipeline()
        key = self.redis_key(symbol, start)
        redis_data = []

        for i in data:
            redis_data.append(i.to_string())

        if redis_data:
            pipe.rpush(key, *redis_data)
            pipe.expire(key, 86400)
        else:
            print('no data.(might be weekends?)')

        pipe.sadd(self._memo_key(key), key)
        pipe.expire(self._memo_key(key), 86400)

        try:
            pipe.execute(True)
        except Exception as e:
            print(f"發生錯誤: {traceback.format_exc()}")
            return False
        return True

    @override
    def _get_data_from_api(self, contract, start, end=None) -> Ticks:
        ticks = self.api.ticks(
            contract,
            start,
            TicksQueryType.AllDay,
        )
        return ticks

    @override
    def _prepare_data(self, contract, start, end=None):
        symbol = contract.symbol

        self.log('Fetching..')
        ticks = self._get_data_from_api(contract, start)

        self.log('Set ticks to db..')
        suc, history_ticks = self._set_data_to_db(ticks, symbol, start)

        if not suc:
            return False, []

        self.log('Set ticks to redis..')
        suc = self._set_data_to_redis(history_ticks, symbol, start)

        if not suc:
            return False, []

        return True, history_ticks

    def log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)
