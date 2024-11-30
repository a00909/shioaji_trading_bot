import datetime

from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.realtime_tick_manager import RealtimeTickManager


class TickManager:
    def __init__(self, api, redis, contract, session_maker, start=False):
        self.rtm = RealtimeTickManager(api, redis, contract)
        self.htm = HistoryTickManager(api, redis, session_maker)
        self.start_time = None
        self.redis = redis
        if start:
            self.start()

    def start(self):
        self.rtm.start()
        self.rtm.tick_received_event.wait()
        self.rtm.tick_received_event.clear()
        self.start_time = self.rtm.get_ticks_by_index(0)[0].datetime

    def stop(self):
        self.rtm.stop()

    def get_tick_by_last_cnt(self):
        pass

    def get_tick_py_timedelta_backwards(self):
        pass
