from shioaji import KBars

from data_manager.history.base2._api_fetcher_base import _ApiFetcherBase
from data_manager.history.statics.kbar._kbar_spec import kbar_spec
from data_manager.history.statics.kbar.np_kbars import NPKBars


class ApiFetcher(_ApiFetcherBase[NPKBars]):
    def __init__(self, api):
        super().__init__(api, kbar_spec)

    def _fetch_from_api(self, contract, start):
        kbars:KBars = self._api.kbars(
            contract,
            start.strftime('%Y-%m-%d'),
            start.strftime('%Y-%m-%d'),
        )
        return kbars
