from contextlib import closing
from datetime import date

from data_manager.history.htm2._api_fetcher import ApiFetcher
from tools.app.app import App
from tools.utils import tmf_r1_contract

app = App()
api_fetcher = ApiFetcher(app.api)
contract = tmf_r1_contract(app.api)
with closing(app.engine.raw_connection()) as conn:
    api_fetcher.fetch(conn, contract, {
        date(2026, 5, 20),
        # date(2026, 4, 14),
        # date(2026, 3, 31),
    })
