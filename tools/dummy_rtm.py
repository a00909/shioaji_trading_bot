from bisect import bisect_left
from datetime import timedelta

from line_profiler_pycharm import profile
from redis.client import Redis
from shioaji import TickFOPv1

from data.tick_fop_v1d1 import TickFOPv1D1
from database.schema.history_tick import HistoryTick
from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tools.utils import get_now


class DummyRealtimeTickManager:

    def __init__(self, contract, htm: HistoryTickManager, redis, simu_date: str = '2024-12-13'):
        self.simu_date = simu_date
        self.htm: HistoryTickManager = htm
        self.contract = contract
        self.r_ticks: list[TickFOPv1D1] = None
        self.pointer = -1
        self.last_print_process = None
        self.last_print_time = None
        self.redis_key = f'dummy:rts:{simu_date}'
        self.redis: Redis = redis
        self.btg = BacktrackingTimeGetter(self.redis, lambda: self.redis_key)
        self.total_ticks_num = 0
        self.dts = []
        self.last_query_max_ticks_num = 0
        self.last_left = None
        self.last_right = None

    def start(self):

        if not self.redis.exists(self.redis_key):
            h_ticks = self.htm.get_tick(contract=self.contract, date=self.simu_date)

            data = {}
            for e, t in enumerate(h_ticks):
                tv1d1 = t.to_tickfopv1d1()
                data[tv1d1.serialize(e)] = tv1d1.datetime.timestamp()
                self.dts.append(tv1d1.datetime)
            self.redis.zadd(self.redis_key, data)

            self.total_ticks_num = len(h_ticks)

        else:
            self.dts = [TickFOPv1D1.deserialize(d).datetime for d in self.redis.zrange(self.redis_key, 0, -1)]
            self.total_ticks_num = self.redis.zcard(self.redis_key)

        self.pointer = 25000
        # self.r_ticks = [ht.to_tickfopv1d1() for ht in h_ticks]

    def stop(self):
        pass

    def wait_for_ready(self):
        pass

    # functions
    def wait_for_tick(self):
        if self.pointer < self.total_ticks_num - 1 and self.pointer < 40000:

            self.pointer += 1
            if self.pointer % 593 == 0:
                now = get_now()
                print(
                    f'process: {self.pointer}/{self.total_ticks_num}, '
                    f'last query: {self.last_query_max_ticks_num} , '
                    f'time consume: {now - self.last_print_process if self.last_print_process else ""},\n'
                    f'l: {self.last_left.datetime}, r: {self.last_right.datetime}\n'

                )
                self.last_left = None
                self.last_right = None
                self.last_query_max_ticks_num = 0
                self.last_print_process = now
            return True
        else:
            return False

    @profile
    def get_ticks_by_backtracking_time(self, backtracking_time: timedelta) -> list[TickFOPv1D1]:
        end = self.dts[self.pointer]

        start = end - backtracking_time

        # data = self.redis.zrangebyscore(self.redis_key, start.timestamp(), end.timestamp(), withscores=False)
        # return [TickFOPv1D1.deserialize(d) for d in data]

        results = self.btg.get(start, end)

        # for test
        self.last_query_max_ticks_num = max(self.last_query_max_ticks_num, len(results))
        if not self.last_left:
            self.last_left = results[0]
            self.last_right = results[-1]
        else:
            self.last_left = min(self.last_left, results[0])
            self.last_right = max(self.last_right, results[-1])
        return results

    def get_ticks_by_backward_idx(self, backward_idx=0) -> list[TickFOPv1D1]:
        data = self.redis.zrange(self.redis_key, -1 - backward_idx, -1)

        return [TickFOPv1D1.deserialize(tick) for tick in data]

    def get_ticks_by_index(self, start=0, end=-1) -> list[TickFOPv1D1]:
        data = self.redis.zrange(self.redis_key, start=start, end=end)

        return [TickFOPv1D1.deserialize(tick) for tick in data]
