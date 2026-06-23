from shioaji import Shioaji

from data_manager.history.base2._npy_cached_data_manager_base import _NpyCachedDataManagerBase
from data_manager.history.htm2 import HistoryTickManager2
from data_manager.history.statics.tick._tick_spec import tick_spec
from data_manager.history.statics.tick.np_ticks import NPTicks


class NpyCachedHistoryTickManager(_NpyCachedDataManagerBase[NPTicks]):
    _cache_key_infix = 'tick'
    def __init__(self, api: Shioaji):
        super().__init__(tick_spec, HistoryTickManager2(api))
