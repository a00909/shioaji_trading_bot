from shioaji import Shioaji

from data_manager.history.base2._npy_cached_data_manager_base import _NpyCachedDataManagerBase
from data_manager.history.statics.kbar._kbar_spec import kbar_spec
from data_manager.history.statics.kbar.np_kbars import NPKBars
from data_manager.history.kbm2 import KBarManager2


class NpyCachedKBarManager(_NpyCachedDataManagerBase[NPKBars]):
    _cache_key_infix = 'kbar'

    def __init__(self, api: Shioaji):
        super().__init__(kbar_spec, KBarManager2(api))
