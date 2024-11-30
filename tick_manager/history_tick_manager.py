import traceback
from datetime import datetime, timedelta

from redis.client import Redis
from shioaji import Shioaji
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker

from database.schema.history_tick import HistoryTickMemo, HistoryTick


class HistoryTickManager:
    memo_key = 'history.tick.memo'
    history_ticks_key_prefix = 'history.tick'
    date_format_redis = '%Y.%m.%d'
    date_format_db = '%Y-%m-%d'

    def __init__(self, api, redis, session_maker: sessionmaker[Session], log_on=True):
        self.log_on = log_on
        self.api: Shioaji = api
        self.redis: Redis = redis
        self.session_maker: sessionmaker[Session] = session_maker

    def redis_key(self, symbol, date):
        return f'{self.history_ticks_key_prefix}:{symbol}:{date}'

    def set_ticks_to_redis(self, ticks: list[HistoryTick], symbol, date):
        pipe = self.redis.pipeline()
        key = self.redis_key(symbol, date)
        data = []

        for i in ticks:
            data.append(i.to_string())

        pipe.lpush(key, *data)
        pipe.sadd(self.memo_key, key)

        pipe.expire(key, 86400)
        pipe.expire(self.memo_key, 86400)

        try:
            pipe.execute(True)
        except Exception as e:
            print(f"發生錯誤: {traceback.format_exc()}")
            return False

        return True

    def get_tick_from_redis(self, symbol, date):
        key = self.redis_key(symbol, date)

        if self.redis.sismember(self.memo_key, key):
            results = self.redis.lrange(key, 0, -1)
            return True, results
        return False, []

    def create_partition_table(self, session: Session, date_str: str):
        date = datetime.strptime(date_str, self.date_format_db)

        partition_name = f'history_tick_{date.strftime("%Y%m")}'

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
                        CREATE TABLE %I PARTITION OF history_tick FOR VALUES FROM (%L) TO (%L)',
                        '{partition_name}', 
                        '{start_date.strftime(self.date_format_db)}', 
                        '{end_date.strftime(self.date_format_db)}'
                    );
                END IF;
            END $$;
            """
        session.execute(text(sql))

    def set_ticks_to_db(self, ticks, symbol, date):
        ticks_len = len(ticks.ts)

        new_ticks = []
        for i in range(ticks_len):
            ts = float(ticks.ts[i]) / 1000000000
            new_ticks.append(HistoryTick(
                ts=datetime.fromtimestamp(ts),
                symbol=symbol,
                close=ticks.close[i],
                volume=ticks.volume[i],
                bid_price=ticks.bid_price[i],
                bid_volume=ticks.bid_volume[i],
                ask_price=ticks.ask_price[i],
                ask_volume=ticks.ask_volume[i],
                tick_type=ticks.tick_type[i],
            ))
        memo = HistoryTickMemo(
            date=datetime.strptime(date, self.date_format_db).date(),
            symbol=symbol
        )

        with self.session_maker() as session:
            try:
                self.create_partition_table(session, date)
                session.add_all(new_ticks)
                session.add(memo)
                session.commit()

            except Exception as e:
                # 捕獲並處理例外
                print(f"發生錯誤: {traceback.format_exc()}")
                session.rollback()
                return False, []
        return True, new_ticks

    def get_tick_from_db(self, symbol, date):
        with self.session_maker() as session:
            memo = session.query(HistoryTickMemo).get(
                {"date": datetime.strptime(date, self.date_format_db).date(), "symbol": symbol}
            )
            if memo:
                # 將字串轉換為datetime對象
                date_obj = datetime.strptime(date, self.date_format_db)

                previous_day = date_obj - timedelta(days=1)

                start_time = previous_day.replace(hour=15, minute=0, second=0)
                end_time = date_obj.replace(hour=13, minute=45, second=0)
                results = session.query(HistoryTick).filter(
                    HistoryTick.ts.between(start_time, end_time),
                    HistoryTick.symbol == symbol
                ).all()

                return True, results
            return False, []

    def get_tick_from_api(self, contract, date) -> Ticks:
        ticks = self.api.ticks(
            contract,
            date,
            TicksQueryType.AllDay,
        )
        return ticks

    def prepare_ticks(self, contract, date):
        symbol = contract.symbol

        self.log('Fetching..')
        ticks = self.get_tick_from_api(contract, date)

        self.log('Set ticks to db..')
        suc, history_ticks = self.set_ticks_to_db(ticks, symbol, date)

        if not suc:
            return False, []

        self.log('Set ticks to redis..')
        suc = self.set_ticks_to_redis(history_ticks, symbol, date)

        if not suc:
            return False, []

        return True, history_ticks

    def log(self, *args, **kwargs):
        if self.log_on:
            print(*args, **kwargs)

    def get_tick(self, contract, date: str) -> list[HistoryTick]:
        symbol = contract.symbol

        self.log('Try load ticks from redis...')
        suc, history_ticks = self.get_tick_from_redis(symbol, date)
        if suc:
            return [HistoryTick.from_string(ht) for ht in history_ticks]

        self.log('Try load ticks from db...')
        suc, history_ticks = self.get_tick_from_db(symbol, date)
        if suc:
            self.log('DB -> redis...')
            self.set_ticks_to_redis(history_ticks, symbol, date)
            return history_ticks

        # fetch new data
        self.log('Fetch ticks from api...')
        suc, history_ticks = self.prepare_ticks(contract, date)
        if not suc:
            return []

        return history_ticks
