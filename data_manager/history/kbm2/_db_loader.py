from data_manager.history.base2._db_loader_base import _DbLoaderBase
from data_manager.history.statics.kbar._kbar_spec import kbar_spec
from data_manager.history.statics.kbar.np_kbars import NPKBars


class DBLoader(_DbLoaderBase[NPKBars]):
    def __init__(self):
        super().__init__(kbar_spec)
