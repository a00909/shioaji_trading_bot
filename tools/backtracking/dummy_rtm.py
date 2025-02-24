from bisect import bisect_left, bisect_right
from datetime import timedelta, datetime

from line_profiler_pycharm import profile
from redis.client import Redis
from shioaji import TickFOPv1

from data.tick_fop_v1d1 import TickFOPv1D1
from database.schema.history_tick import HistoryTick
from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.rtm.rtm_base import RealtimeTickManagerBase
from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tools.utils import get_now


class DummyRealtimeTickManager(RealtimeTickManagerBase):

    def __init__(self, contract, htm: HistoryTickManager, redis, simu_date: str = '2024-12-13'):
        super().__init__()
        self.simu_date = simu_date
        self.htm: HistoryTickManager = htm
        self.contract = contract
        self.r_ticks: list[TickFOPv1D1] = None

        self.last_print_process = None
        self.last_print_time = None
        self.redis_key = f'dummy:rts:{simu_date}'
        self.redis: Redis = redis
        self.btg = BacktrackingTimeGetter(self.redis, lambda: self.redis_key)

        self.dts = []
        self.last_query_max_ticks_num = 0
        self.last_left = None
        self.last_right = None

    @property
    def symbol(self):
        return self.contract.symbol

    @property
    def start_time(self):
        return self.buffer[0].datetime

    def start(self, *args, **kwargs):
        h_ticks = self.htm.get_tick(contract=self.contract, date=self.simu_date)
        self.buffer = [t.to_tickfopv1d1() for t in h_ticks]
        # self.pointer = 25000

    def stop(self):
        pass

    def wait_for_ready(self):
        pass

    def _print_msg(self):
        if self.window_right % 3758 == 0:
            now = get_now()
            print(
                f'process: {self.window_right}/{len(self.buffer)}, '
                f'last query: {self.last_query_max_ticks_num} , '
                f'time consume: {now - self.last_print_process if self.last_print_process else ""},\n'
                # f'l: {self.last_left.datetime}, r: {self.last_right.datetime}\n'

            )
            self.last_left = None
            self.last_right = None
            self.last_query_max_ticks_num = 0
            self.last_print_process = now

    # functions
    def wait_for_tick(self):
        if self.window_right <  len(self.buffer) - 1:
            self._print_msg()

            self.window_right += 1
            self._update_window_left()

            return True
        else:
            return False
