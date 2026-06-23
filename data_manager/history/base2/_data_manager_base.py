from abc import ABC, abstractmethod
from datetime import date

from psycopg import Connection
from shioaji.contracts import Contract

from data_manager.history._utils import get_missing_dates, range_check
from data_manager.history.base2._api_fetcher_base import _ApiFetcherBase
from data_manager.history.base2._db_loader_base import _DbLoaderBase
from data_manager.history.statics.base._np_data_base import _NpDataBase
from tools.date_range_utils import enumerate_dates_set_by_range


class _DataManagerBase[D:_NpDataBase](ABC):
    def __init__(self, fetcher, db_loader):
        self._fetcher: _ApiFetcherBase = fetcher
        self._db_loader: _DbLoaderBase = db_loader

    def get_ticks(
            self,
            conn: Connection,
            contract: Contract,
            start: date = None,
            end: date = None,
            dates: set[date] = None
    ) -> dict[date, D]:

        if start and end:
            range_check(start, end)
            dates = enumerate_dates_set_by_range(start, end)
        elif dates:
            if len(dates) == 1:
                range_check(max(dates))
            elif len(dates) > 1:
                range_check(min(dates), max(dates))
        else:
            raise Exception('no range or dates given.')

        missing_dates: set[date] = get_missing_dates(conn, contract.symbol, self.memo_table_name, dates)
        ticks_from_api: dict[date, D] = self._fetcher.fetch(
            conn,
            contract,
            missing_dates
        ) if missing_dates else {}

        db_dates = dates - missing_dates
        if db_dates:
            ticks_from_db: dict[date, D] = self._db_loader.load(conn, contract.symbol, db_dates)
        else:
            ticks_from_db = {}

        return ticks_from_api | ticks_from_db

    @property
    @abstractmethod
    def memo_table_name(self) -> str:
        ...
