import threading

from strategy.tools.order_placer import OrderPlacer
from tick_manager.realtime_tick_manager import RealtimeTickManager
from strategy.runner.tmf_strategy_runner import TMFStrategyRunner
from tools.app import App
from tick_manager.history_tick_manager import HistoryTickManager

app = App(init=True)
print(app.api.futopt_account)

# contract = app.api.Contracts.Futures.TMF.TMFR1
contract = app.api.Contracts.Futures.TXF.TXFR1
rtm = RealtimeTickManager(app.api, app.redis, contract)
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
op = OrderPlacer(app.api, contract, app.api.futopt_account)
stra = TMFStrategyRunner(rtm, htm, op)


stra.start()


def loop():
    while True:
        msg = (
            f'enter "e" to exit.'
        )
        i = input(msg)

        match i:
            case 'e':
                stra.stop()
                # rtm.stop()
                app.shut()
                print('bye bye.')
                break
            case _:
                pass


threading.Thread(target=loop).start()
