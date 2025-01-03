import datetime
from bisect import bisect_left, bisect_right
from typing import Callable

from line_profiler_pycharm import profile

from data.tick_fop_v1d1 import TickFOPv1D1
from tools.utils import get_now


class BacktrackingTimeGetter:
    def __init__(self, redis, get_key: Callable[[], str], remove_old=True):
        self.last_start: datetime = None
        self.last_end: datetime = None
        self.get_key: Callable[[], str] = get_key
        self.redis = redis
        self.buffer: list[TickFOPv1D1] = []
        self.max_buffer_size = 2197152
        self.expire_limit = datetime.timedelta(hours=5)
        self.remain_range = datetime.timedelta(hours=4)
        self.remove_old = remove_old

    def remove_old_data(self):
        now = get_now()
        expire_time = now - self.expire_limit

        if not self.last_start or self.last_start >= expire_time:
            return

        new_start = now - self.remain_range

        new_start_idx = bisect_left(self.buffer, new_start)

        if new_start_idx == -1:
            raise Exception('Should not be here!')

        self.buffer = self.buffer[new_start_idx:]
        self.last_start = new_start

    def get(self, start: datetime, end: datetime, with_start=True, with_end=True) -> list[TickFOPv1D1]:
        results = self.__get(start, end)
        if not results:
            return []

        # 如果不包含 start
        if not with_start:
            cur = 0
            while cur < len(results) and start >= results[cur].datetime:
                cur += 1
            results = results[cur:]

        # 如果不包含 end
        if not with_end:
            cur = len(results) - 1
            while cur >= 0 and end <= results[cur].datetime:
                cur -= 1
            results = results[:cur + 1]

        return results

    @profile
    def __get(self, start, end) -> list[TickFOPv1D1]:
        # end = get_now()
        # start = end - length
        if self.remove_old:
            self.remove_old_data()

        key = self.get_key()

        if self.last_start:
            if self.last_start <= start <= self.last_end and self.last_start <= end <= self.last_end:

                l = bisect_left(self.buffer, start)
                r = bisect_right(self.buffer, end)

                return self.buffer[l:r]

            elif self.last_start <= start <= end:  # last_end < end
                l_score = self.last_end
                r_score = end
                data = [(l_score, r_score)]
                new_start = l_score
                new_end = r_score
                ret_r = None
                ret_l = -1
            elif start <= end <= self.last_end:
                l_score = start
                r_score = self.last_start
                data = [(l_score, r_score)]
                new_start = l_score
                new_end = r_score
                ret_l = 0
                ret_r = -1
            elif start <= self.last_start and self.last_end <= end:
                l_score1 = start
                r_score1 = self.last_start
                l_score2 = self.last_end
                r_score2 = end
                data = [
                    (l_score1, r_score1), (l_score2, r_score2)
                ]
                new_start = l_score1
                new_end = r_score2
                ret_l = 0
                ret_r = None

            else:
                raise Exception(
                    f'start: {start} | '
                    f'end: {end} | '
                    f'last_start: {self.last_start} | '
                    f'last_end: {self.last_end}'
                )

            for d in data:
                new_data_raw = self.redis.zrangebyscore(key, d[0].timestamp(), d[1].timestamp(), withscores=False)
                new_data: list[TickFOPv1D1] = [TickFOPv1D1.deserialize(nd) for nd in new_data_raw]

                if d[0] <= d[1] == self.last_start:
                    joint_idx = bisect_right(self.buffer, d[1])
                    self.buffer = new_data + self.buffer[joint_idx:]
                elif self.last_end == d[0] <= d[1]:
                    joint_idx = bisect_left(self.buffer, d[0])
                    self.buffer = self.buffer[:joint_idx] + new_data
                else:
                    raise Exception()

            self.last_start = min(new_start, self.last_start)
            self.last_end = max(new_end, self.last_end)

            if -1 == ret_l:
                ret_l = bisect_left(self.buffer, start)
            if -1 == ret_r:
                ret_r = bisect_right(self.buffer, end)

            return self.buffer[ret_l:ret_r]

        else:
            new_data_raw = self.redis.zrangebyscore(key, start.timestamp(), end.timestamp(), withscores=False)
            new_data: list[TickFOPv1D1] = [TickFOPv1D1.deserialize(nd) for nd in new_data_raw]
            self.last_start = start
            self.last_end = end
            self.buffer = new_data

            return self.buffer
