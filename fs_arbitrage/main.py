from threading import Event, Lock

from fs_arbitrage.pair_container import PairContainer
from fs_arbitrage.variety_unit import VarietyUnit
from tools.app import App


class Main:
    def __init__(self, app: App) -> None:
        app = App(init=True)
        api = app.api
        redis = app.redis
        account = app.get_default_account()

        self.dirty_set = set()
        self.dirty_lock = Lock()
        self.new_data_event = Event()

        self.running = False

        contracts = [
            app.api.Contracts.Futures.TXF.TXFR1,
            app.api.Contracts.Futures.MXF.MXFR1,
            app.api.Contracts.Futures.TMF.TMFR1,
        ]

        units = []
        for c in contracts:
            units.append(VarietyUnit(

            ))

        self.pair_container = PairContainer()

    def stop(self):
        self.running = False


    def _unit_emitter(self, code: str):
        self.dirty_set.add(code)
        self.new_data_event.set()

    def start(self) -> None:
        self.running = True

        while self.running:
            self.new_data_event.wait()
            self.new_data_event.clear()
            dirties, self.dirty_set = self.dirty_set, set()

            pairs = self.pair_container.get_pairs_by_code_batch(dirties)







if __name__ == '__main__':
    pass
