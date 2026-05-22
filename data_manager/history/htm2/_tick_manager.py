from datetime import date

from psycopg import Connection
from shioaji import Shioaji
from shioaji.contracts import Contract

from data_manager.history._utils import get_missing_dates, range_check
from data_manager.history.htm2._api_fetcher import ApiFetcher
from data_manager.history.htm2._db_loader import DBLoader
from data_manager.history.statics.np_ticks import NPTicks
from database.schema.history_tick import HistoryTickMemo
from tools.date_range_utils import enumerate_dates_set_by_range


class TickManager:

    def __init__(self, api):
        self._api: Shioaji = api
        self._fetcher = ApiFetcher(api)
        self._db_loader = DBLoader()

    def get_ticks(
            self,
            conn: Connection,
            contract: Contract,
            start: date = None,
            end: date = None,
            dates: set[date] = None
    ) -> dict[date, NPTicks]:

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

        missing_dates: set[date] = get_missing_dates(conn, contract.symbol, HistoryTickMemo.__tablename__, dates)
        ticks_from_api: dict[date, NPTicks] = self._fetcher.fetch(
            conn,
            contract,
            missing_dates
        ) if missing_dates else {}

        db_dates = dates - missing_dates
        if db_dates:
            ticks_from_db: dict[date, NPTicks] = self._db_loader.load(conn, contract.symbol, db_dates)
        else:
            ticks_from_db = {}

        return ticks_from_api | ticks_from_db
