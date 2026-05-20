from datetime import date

from data_manager.history_data_manager.history_tick_manager2._api_fetcher import ApiFetcher
from tools.app.app import App
from tools.utils import tmf_r1_contract

app = App()
api_fetcher = ApiFetcher(app.api, app.engine)
contract = tmf_r1_contract(app.api)
api_fetcher.fetch(contract, {
    date(2026, 4, 13),
    # date(2026, 4, 14),
    # date(2026, 3, 31),
})
