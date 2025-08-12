from datetime import date, timedelta, datetime

from redis.client import Redis
from shioaji import Shioaji
from shioaji.data import Kbars
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker, Session

from database.schema.kbar import KBar, KBarMemo
from tools.constants import DATE_FORMAT_SHIOAJI
from tools.utils import get_now
from concurrent.futures.thread import ThreadPoolExecutor

class KBarManager:
    def __init__(self, api: Shioaji, redis: Redis, session: sessionmaker[Session]):
        self.api: Shioaji = api
        self.redis = redis
        self.session_maker: sessionmaker[Session] = session
        self.tpe = ThreadPoolExecutor(max_workers=8)

    def _get_from_api(self, contract, start: str, end: str) -> Kbars:
        res: Kbars = self.api.kbars(
            contract,
            start,
            end,
        )
        return res

    def _save_kbars_to_db(self,symbol, kbars_list: list[Kbars]):
        db_kbars:list[KBar] = []
        for kbars in kbars_list:
            for i in range(len(kbars.ts)):
                kbar = KBar()
                kbar.ts = kbars.ts[i]
                kbar.open = kbars.Open[i]
                kbar.close = kbars.Close[i]
                kbar.high = kbars.High[i]
                kbar.low = kbars.Low[i]
                kbar.volume = kbars.Volume[i]
                kbar.amount = kbars.Amount[i]
                kbar.symbol = symbol
                db_kbars.append(KBar())

    def _save_kbar_memos_to_db(self):
        pass

    def _get_from_db(self):
        pass

    def _save_to_redis(self):
        pass

    def _get_from_redis(self):
        pass

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

    def _date_check(self, start, end):
        if start > end:
            raise Exception("start cannot greater than end!")

        now = get_now()
        date_dt = datetime.strptime(end, DATE_FORMAT_SHIOAJI)
        if not (
                date_dt.date() < now.date() or
                (
                        # now == 要求日期且早盤已收
                        now.date().strftime(DATE_FORMAT_SHIOAJI) == end
                        and
                        (
                                (now.hour == 13 and now.minute >= 45)
                                or
                                now.hour >= 14
                        )
                )
        ):
            raise Exception("Kbar is not complete yet.")

    def fetch_kbar_ranges(self, contract, start: date, end: date):

        self._date_check(start,end)

        # Step 1: 查已有 memo
        symbol = contract.symbol
        stmt = select(KBarMemo.date).where(
            KBarMemo.symbol == symbol,
            KBarMemo.date.between(start, end)
        )
        with self.session_maker() as session:
            existing_dates = set(session.execute(stmt).scalars().all())

        # Step 2: 計算所有應該有的日期
        all_dates = {
            start + timedelta(days=i)
            for i in range((end - start).days + 1)
        }

        # Step 3: 找出缺漏
        missing_dates = list(all_dates - existing_dates)
        date_ranges = self._group_dates_into_ranges(missing_dates)

        # Step 4: 批量查 API 並寫入
        data:list[Kbars] = []

        tasks:list = []

        try:
            tasks = [
                self.tpe.submit(
                    self._get_from_api,
                    contract,
                    r_start.strftime('%Y-%m-%d'),
                    r_end.strftime('%Y-%m-%d')
                ) for r_start, r_end in date_ranges
            ]

        except Exception as e:
            session.rollback()
            print(f"API 查詢失敗: {symbol} {start} ~ {end}: {e}")

        data =[t.result() for t in tasks]
        # 寫入資料（依你自己的處理邏輯）
        self._save_kbars_to_db(symbol,data)
        self._save_kbar_memo_to_db()

        # 無論有無資料，標記每個日期為已查
        d = r_start
        while d <= r_end:
            session.add(KBarMemo(symbol=symbol, date=d))
            d += timedelta(days=1)

        session.commit()
