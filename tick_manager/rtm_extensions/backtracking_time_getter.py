import datetime
from bisect import bisect_left, bisect_right
from typing import Callable

from line_profiler_pycharm import profile

from data.tick_fop_v1d1 import TickFOPv1D1
from tools.utils import get_now


class BacktrackingTimeGetter:
    def __init__(self, redis, get_key: Callable[[], str]):
        self.last_start: datetime = None
        self.last_end: datetime = None
        self.get_key: Callable[[], str] = get_key
        self.redis = redis
        self.buffer: list[TickFOPv1D1] = []
        self.max_buffer_size = 2197152

    @profile
    def get(self, start, end) -> list[TickFOPv1D1]:
        # end = get_now()
        # start = end - length
        key = self.get_key()

        if self.last_start:
            if self.last_start <= start <= self.last_end and self.last_start <= end <= self.last_end:

                l = bisect_left(self.buffer, start)
                r = bisect_right(self.buffer, end)

                return self.buffer[l:r]

            elif self.last_start <= start < end:  # last_end < end
                l_score = self.last_end
                r_score = end
                data = [(l_score, r_score)]
                new_start = l_score
                new_end = r_score
                ret_r = None
                ret_l = -1
            elif start < end <= self.last_end:
                l_score = start
                r_score = self.last_start
                data = [(l_score, r_score)]
                new_start = l_score
                new_end = r_score
                ret_l = 0
                ret_r = -1
            elif start < self.last_start and self.last_end < end:
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
                raise Exception()

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
