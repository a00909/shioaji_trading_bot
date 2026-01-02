import heapq
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date, timedelta, datetime

from redis.client import Redis
from shioaji import Shioaji
from shioaji.data import Kbars
from sqlalchemy import select, or_, and_
from sqlalchemy.orm import sessionmaker, Session

from database.schema.kbar import KBar, KBarMemo
from tick_manager.history_data_manager_base import HistoryDataManagerBase
from tools.constants import DATE_FORMAT_DB_AND_SJ, EXP86400
from tools.kbar_utils import to_time_key
from tools.utils import history_ts_to_datetime


class KBarManager(HistoryDataManagerBase[KBar]):

    def __init__(self, api: Shioaji, redis: Redis, session_maker: sessionmaker[Session], log_on=True):
        super().__init__(api, redis, session_maker, log_on)

        self.tpe = ThreadPoolExecutor(max_workers=8)

    @property
    def _redis_key_prefix(self):
        return 'kbar'

    def _get_data_from_db(self, symbol, missing_ranges: list[tuple[date, date]]) -> list[KBar]:
        if not missing_ranges:
            return []
        session = self.session_maker()

        results = session.query(KBar).filter(
            KBar.symbol == symbol,  # noqa
            or_(*(
                and_(KBar.ts >= s, KBar.ts < e + timedelta(days=1))
                for s, e in missing_ranges
            )),
        ).order_by(KBar.ts).all()

        return results  # noqa

    def _set_data_to_db(self, data, symbol, ranges: list[tuple[date, date]]):
        db_kbars: list[KBar] = []
        for kbars in data:
            for i in range(len(kbars.ts)):
                kbar = KBar()
                kbar.ts = history_ts_to_datetime(kbars.ts[i])
                kbar.open = kbars.Open[i]
                kbar.close = kbars.Close[i]
                kbar.high = kbars.High[i]
                kbar.low = kbars.Low[i]
                kbar.volume = kbars.Volume[i]
                kbar.amount = kbars.Amount[i]
                kbar.symbol = symbol
                db_kbars.append(kbar)

        first_days = []
        first_days_memo = set()
        memos = []

        for s, e in ranges:
            # 產生所有日期 (每天一筆)
            memos.extend(
                KBarMemo(date=cur, symbol=symbol)
                for i in range((e - s).days + 1)
                for cur in [s + timedelta(days=i)]
            )

            # 紀錄該 range 裡遇到的新月份
            cur = s
            while cur <= e:
                ym = (cur.year, cur.month)
                if ym not in first_days_memo:
                    first_days_memo.add(ym)
                    first_days.append(cur)  # 用這個月遇到的第一天當代表
                # 直接跳到下個月第一天 → 減少迴圈次數
                if cur.month == 12:
                    cur = cur.replace(year=cur.year + 1, month=1, day=1)
                else:
                    cur = cur.replace(month=cur.month + 1, day=1)

        suc = self._commit_to_db(KBar.__tablename__, first_days, db_kbars, memos)
        return (suc, db_kbars) if suc else (suc, [])

    def _get_data_from_redis(self, symbol, start: date, end: date):
        # Ensure all necessary data are in Redis.
        pipe = self.redis.pipeline()

        cur = start
        while cur <= end:
            key = self._redis_key(symbol, cur)
            pipe.hgetall(key)
            cur += timedelta(days=1)

        data = [KBar.from_string(str_data) for _, str_data in pipe.execute(True)]
        data.sort()

        return data

    def _set_data_to_redis(self, data: list[KBar], symbol, ranges: list[tuple[date, date]]):
        """

        :param data: 資料，默認已排序
        :param symbol: 資料的記號，例如2330
        :param ranges: 加入資料的期間，可以是多個
        :return:
        """
        if not data:
            self._log('no kbar to set to redis.')
            return False

        group_by_date: dict[date, list[KBar]] = {}
        for d in data:
            kbar_date = d.ts.date()
            if kbar_date not in group_by_date:
                group_by_date[kbar_date] = []
            group_by_date[kbar_date].append(d)

        pipe = self.redis.pipeline()

        for s, e in ranges:
            cur = s
            while cur <= e:
                redis_key = self._redis_key(
                    symbol,
                    cur
                )
                pipe.set(self._memo_key(redis_key), self.redis_memo_default_value, ex=EXP86400)

                if cur in group_by_date:
                    for kbar in group_by_date[cur]:
                        pipe.hset(redis_key, to_time_key(kbar.ts), kbar.to_string())
                    pipe.expire(redis_key, EXP86400)
                else:
                    self._log(f'no kbar data: {cur.strftime(DATE_FORMAT_DB_AND_SJ)}')

                cur += timedelta(days=1)

        return pipe.execute(True)

    def _get_data_from_api(self, contract, start: str, end: str):
        res: KBar = None
        try:
            res: Kbars = self.api.kbars(
                contract,
                start,
                end,
            )
        except Exception as e:
            print(f"API 查詢失敗: {contract.symbol} {start} ~ {end}: {e}")

        return res

    def _prepare_db_data(self, contract, ranges):
        symbol = contract.symbol

        tasks = [
            self.tpe.submit(
                self._get_data_from_api,
                contract,
                r_start.strftime('%Y-%m-%d'),
                r_end.strftime('%Y-%m-%d')
            ) for r_start, r_end in ranges
        ]

        data = [t.result() for t in tasks]

        suc, db_kbars = self._set_data_to_db(data, symbol, ranges)
        if not suc:
            return False, []

        return True, db_kbars

    def get_data(self, contract, start: str | date, end: str | date) -> list[KBar]:
        self._date_check(start, end)
        symbol = contract.symbol
        if isinstance(start, date) and isinstance(end, date):
            pass
        elif isinstance(start, datetime) and isinstance(end, datetime):
            start = start.date()
            end = end.date()
        elif isinstance(start, str) and isinstance(end, str):
            start = self._dt(start).date()
            end = self._dt(end).date()
        else:
            raise Exception('type error with start or end.')

        missing_ranges_db = self._find_missing_ranges_db(symbol, start, end)
        api_data = []
        if missing_ranges_db:
            suc, api_data = self._prepare_db_data(contract, missing_ranges_db)
            if not suc:
                raise Exception('error while preparing db data.')

        missing_ranges_redis, existing_data = self._find_missing_data_redis(
            symbol,
            start,
            end,
            returns_existing_data=True
        )
        db_data = []
        if missing_ranges_redis:
            missing_ranges_redis = self.subtract_ranges(missing_ranges_redis, missing_ranges_db)
            db_data = self._get_data_from_db(symbol, missing_ranges_redis)

        self._set_data_to_redis(api_data, symbol, missing_ranges_db)
        self._set_data_to_redis(db_data, symbol, missing_ranges_redis)

        api_data.sort()
        db_data.sort()
        existing_data.sort()
        merged = list(heapq.merge(api_data, db_data, existing_data))

        return merged

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

    def _find_missing_ranges_db(self, symbol, start: date, end: date):

        # Step 1: 查已有 memo
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
        self._log(
            f'All ranges: {[(s.strftime(DATE_FORMAT_DB_AND_SJ), t.strftime(DATE_FORMAT_DB_AND_SJ)) for s, t in date_ranges]}'
        )

        return date_ranges

    def _find_missing_data_redis(self, symbol, start: date, end: date, returns_existing_data=False):
        pipe = self.redis.pipeline()

        # 預先建立日期與對應的 redis key
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        keys = [self._redis_key(symbol, d) for d in dates]

        # 檢查哪些 key 已存在
        for k in keys:
            pipe.exists(self._memo_key(k))

        memos = pipe.execute()
        missing_dates = []
        existing_keys = []

        for d, k, exists in zip(dates, keys, memos):
            if not exists:
                missing_dates.append(d)
            elif returns_existing_data:
                existing_keys.append(k)

        data = []
        if returns_existing_data and existing_keys:
            pipe = self.redis.pipeline()
            for k in existing_keys:
                pipe.hgetall(k)
            results = pipe.execute()
            data = [KBar.from_string(s) for batch in results for _,s in batch.items()]

        if returns_existing_data:
            return self._group_dates_into_ranges(missing_dates), data

        return self._group_dates_into_ranges(missing_dates)

    @staticmethod
    def subtract_ranges(bigger: list[tuple[date, date]], smaller: list[tuple[date, date]]) -> list[tuple[date, date]]:
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
