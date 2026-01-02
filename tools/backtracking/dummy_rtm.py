from datetime import time

from redis.client import Redis
from typing_extensions import override

from data.unified.tick.tick_fop import TickFOP
from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.rtm.rtm_base import RealtimeTickManagerBase
from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tools.utils import get_now


class DummyRealtimeTickManager(RealtimeTickManagerBase):

    def __init__(self, contract, htm: HistoryTickManager, redis, simu_date: str = '2024-12-13',
                 simu_ranges: list[tuple[time, time]] = None):
        super().__init__()
        self.simu_date = simu_date
        self.htm: HistoryTickManager = htm
        self.contract = contract
        self.r_ticks: list[TickFOP] = None

        self.last_print_process = None
        self.last_print_time = None
        self.redis_key = f'dummy:rts:{simu_date}'
        self.redis: Redis = redis
        self.btg = BacktrackingTimeGetter(self.redis, lambda: self.redis_key)

        self.dts = []
        self.last_query_max_ticks_num = 0
        self.last_left = None
        self.last_right = None

        self.simu_ranges: list[tuple[time, time]] = simu_ranges

    @property
    def symbol(self):
        return self.contract.symbol

    @property
    def start_time(self):
        return self.tick_buffer[0].datetime

    def start(self, *args, **kwargs):
        h_ticks = self.htm.get_data(contract=self.contract, start=self.simu_date, time_ranges=self.simu_ranges)

        for t in h_ticks:
            tick, bidask = t.to_tick_bidask_v1d1()
            self.tick_buffer.append(tick)
            self.bid_ask_buffer.append(bidask)

        # self.pointer = 25000

    def stop(self):
        pass

    def wait_for_ready(self):
        pass

    def _print_msg(self):
        if self.tick_right % 3758 == 0:
            now = get_now()
            print(
                f'process: {self.tick_right}/{len(self.tick_buffer)}, '
                f'last query: {self.last_query_max_ticks_num} , '
                f'time consume: {now - self.last_print_process if self.last_print_process else ""},\n'
                # f'l: {self.last_left.datetime}, r: {self.last_right.datetime}\n'

            )
            self.last_left = None
            self.last_right = None
            self.last_query_max_ticks_num = 0
            self.last_print_process = now

    @override
    def update_window_right(self):
        # if self.tick_right>=0:
        #     old_ts = self.tick_buffer[self.tick_right].datetime
        #     self.tick_right+=1
        #     self.bid_ask_right += 1
        #     count = 0
        #     while self.tick_right< len(self.tick_buffer) - 1 and self.tick_buffer[self.tick_right].datetime == old_ts:
        #         self.tick_right+=1
        #         self.bid_ask_right += 1
        #         count+=1
        #     if count >0:
        #         print(f'{count} ticks skipped.')
        #
        # else:
        self.tick_right += 1
        self.bid_ask_right += 1

    @override
    def wait_for_tick(self):
        if self.tick_right < len(self.tick_buffer) - 1:
            self._print_msg()
            self.update_window()

            return True
        else:
            return False
