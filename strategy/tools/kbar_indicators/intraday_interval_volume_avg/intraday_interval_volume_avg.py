from datetime import date, datetime, timedelta, time

from redis import Redis

from database.schema.kbar import KBar
from tick_manager.kbar_manager import KBarManager
from tools.constants import EXP86400, DATE_FORMAT_REDIS
from tools.kbar_utils import to_time_key


class IntradayIntervalVolumeAvg:
    def __init__(self, contract, kbar_manager, redis):
        self.contract = contract
        self.symbol = contract.symbol
        self.kbar_manager: KBarManager = kbar_manager
        self.redis: Redis = redis
        self.data: dict[str, dict[int, float]] = {}

    def _reids_key(self, end: date | datetime, length: timedelta, interval_min):
        return f'intraday.interval.v.a:{self.symbol}:{interval_min}m:{end.strftime(DATE_FORMAT_REDIS)}:{length.days}D'

    def _memory_date_key(self, end: date | datetime, length: timedelta, interval_min):
        return f'{end.strftime("%Y%m%d")}.{length.days}.{interval_min}'

    def align_minute(self, ts: datetime, interval: int) -> int:
        """
        把時間對齊到 interval 分鐘
        例如 align_time(2025-08-31 09:01, 5) -> 2025-08-31 09:00
        """
        total_minutes = self._total_minutes(ts)
        aligned_minutes = total_minutes - total_minutes % interval
        return aligned_minutes

    @staticmethod
    def _total_minutes(t: time | datetime):
        return t.hour * 60 + t.minute

    def _calc(self, kbars: list[KBar], interval_min, accumulate=False) -> dict[int, float]:
        group_by_minute: dict[int, list[float] | float] = {}

        # 1️⃣ 依分鐘分組
        for k in kbars:
            ts: datetime = k.ts
            key = self.align_minute(ts, interval_min)
            group_by_minute.setdefault(key, []).append(k.volume)

        # 2️⃣ 計算平均值
        for k, v in group_by_minute.items():
            group_by_minute[k] = sum(v) / len(v)

        # 3️⃣ 若不需累積，直接回傳
        if not accumulate:
            return group_by_minute

        # 4️⃣ 累積（固定台指期時段）
        DAY_START = 8 * 60 + 45  # 08:45
        DAY_END = 13 * 60 + 45  # 13:45
        NIGHT_START = 15 * 60  # 15:00
        NIGHT_END = 5 * 60  # 05:00 (跨午夜)

        # 日盤累積
        accu = 0
        for cur in range(DAY_START, DAY_END):
            if cur in group_by_minute:
                accu += group_by_minute[cur]
                group_by_minute[cur] = accu

        # 夜盤累積（跨午夜）
        accu = 0
        for cur in list(range(NIGHT_START, 24 * 60)) + list(range(0, NIGHT_END)):
            if cur in group_by_minute:
                accu += group_by_minute[cur]
                group_by_minute[cur] = accu

        return group_by_minute

    def _to_redis(self, data: dict[int, float], sorted_set_key):
        pipe = self.redis.pipeline()
        for k, v in data.items():
            pipe.hset(sorted_set_key, k, v)  # noqa
        pipe.expire(sorted_set_key, EXP86400)
        return pipe.execute(True)

    def _from_redis(self, end: datetime, length: timedelta, interval_min) -> dict[int, float]:
        key = self._reids_key(end, length, interval_min)

        data = self.redis.hgetall(key)
        result = {}
        for k, v in data.items():
            if isinstance(k, bytes):  # 沒有 decode_responses 的情況
                k = k.decode("utf-8")
                v = v.decode("utf-8")
            result[int(k)] = float(v)
        return result

    def _to_memory(self, data: dict[int, float], end, length, interval_min):
        key = self._memory_date_key(end, length, interval_min)
        self.data[key] = {k: v for k, v in data.items()}

    def _from_memory(self, end: datetime, length: timedelta, interval_min):
        date_key = self._memory_date_key(end, length, interval_min)
        time_key = self.align_minute(end, interval_min)

        if date_key in self.data:
            if time_key in self.data[date_key]:
                return True, self.data[date_key][time_key]
            else:
                return True, 0.0
        return False, None

    def get(self, end: datetime, length: timedelta, interval_min=5):
        """
        算出來是1分K的平均值(之後評估加不同分鐘數的)
        :param end:
        :param length:
        :param interval_min:
        :return:
        """
        s_date = (end - length).date()
        e_date = end.date()

        # 從記憶體中先取
        suc, val = self._from_memory(end, length, interval_min)
        if suc:
            return val

        # 從redis中取
        key = self._reids_key(e_date, length, interval_min)
        if self.redis.exists(key):
            data = self._from_redis(end, length, interval_min)
            self._to_memory(data, end, length, interval_min)
            suc, val = self._from_memory(end, length, interval_min)
            if suc:
                return val
            raise Exception(f'error while getting data from redis.')

        # 無資料從頭算
        kbars = self.kbar_manager.get_data(self.contract, s_date, e_date)
        data = self._calc(kbars, interval_min)
        self._to_redis(data, key)
        self._to_memory(data, end, length, interval_min)
        suc, val = self._from_memory(end, length, interval_min)
        if suc:
            return val
        raise Exception(f'error while calculating indicator.')
