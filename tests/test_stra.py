import threading

from realtime.order_placer import OrderPlacer
from tick_manager.realtime_tick_manager import RealtimeTickManager
from strategy.strategy1 import Strategy1
from utils.app import App
from tick_manager.history_tick_manager import HistoryTickManager

app = App(init=True)
print(app.api.futopt_account)


contract = app.api.Contracts.Futures.TMF.TMFR1
rtm = RealtimeTickManager(app.api, app.redis, contract)
htm = HistoryTickManager(app.api, app.redis, app.session_maker)
op = OrderPlacer(app.api, contract)
stra = Strategy1(rtm, htm, op)
# rtm.start()


# stra.run_strategy()


def loop():
    while True:
        msg = (
            f'type "chk" to check trade status.\n'
            f'"srtm" to start rtm.\n'
            f'"sstra" to start strategy (will also start rtm).\n'
            f'"ca" to close all position and stop stra.\n'
            f'"strtm" to stop rtm.\n'
            f'type "e" to exit.\n'
        )
        i = input(msg)

        match i:
            case 'chk':
                pass
            case 'srtm':
                rtm.start()
            case 'sstra':
                rtm.start()
                stra.run_strategy()
            case 'ca':
                stra.stop_strategy()
            case 'strtm':
                rtm.stop()
            case 'e':
                stra.stop_strategy()
                rtm.stop()
                app.api.logout()
                print('bye bye.')
                break
            case _:
                pass


threading.Thread(target=loop).start()
