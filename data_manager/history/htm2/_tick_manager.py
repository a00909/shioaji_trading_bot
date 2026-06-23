from data_manager.history.base2._data_manager_base import _DataManagerBase
from data_manager.history.htm2._api_fetcher import ApiFetcher
from data_manager.history.htm2._db_loader import DBLoader
from data_manager.history.statics.tick.np_ticks import NPTicks
from database.schema.history_tick import HistoryTickMemo


class TickManager(_DataManagerBase[NPTicks]):
    def __init__(self, api):
        super().__init__(ApiFetcher(api), DBLoader())

    @property
    def memo_table_name(self) -> str:
        return HistoryTickMemo.__tablename__
