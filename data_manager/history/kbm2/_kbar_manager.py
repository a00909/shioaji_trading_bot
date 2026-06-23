from data_manager.history.base2._data_manager_base import _DataManagerBase
from data_manager.history.kbm2._api_fetcher import ApiFetcher
from data_manager.history.kbm2._db_loader import DBLoader
from data_manager.history.statics.kbar.np_kbars import NPKBars
from database.schema.kbar import KBarMemo


class KBarManager(_DataManagerBase[NPKBars]):

    def __init__(self, api):
        super().__init__(ApiFetcher(api), DBLoader())

    @property
    def memo_table_name(self) -> str:
        return KBarMemo.__tablename__
