from shioaji.constant import TicksQueryType

from data_manager.history.base2._api_fetcher_base import _ApiFetcherBase
from data_manager.history.statics.tick._tick_spec import tick_spec
from data_manager.history.statics.tick.np_ticks import NPTicks


class ApiFetcher(_ApiFetcherBase[NPTicks]):
    def __init__(self, api):
        super().__init__(api,  tick_spec)

    def _fetch_from_api(self, contract, start):
        ticks = self._api.ticks(
            contract,
            start.strftime('%Y-%m-%d'),
            TicksQueryType.AllDay,
        )
        return ticks
