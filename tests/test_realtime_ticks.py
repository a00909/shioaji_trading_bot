import threading
from datetime import timedelta

from tick_manager.realtime_tick_manager import RealtimeTickManager
from realtime.utils import to_df
from utils.app import App

app = App(True)
rtm = RealtimeTickManager(
    app.api,
    app.redis,
    app.api.Contracts.Futures.TMF.TMFR1,
)

rtm.start()


def loop():
    while True:
        msg = f'type "chk" to check data.\n' \
              f'type "e" to exit.\n'
        i = input(msg)
        if i.startswith('chk'):
            if i == 'chk':
                sec = 30
            else:
                sec = int(i.replace('chk', '').replace(' ', ''))

            ticks = rtm.get_ticks_by_backtracking_time(timedelta(seconds=sec))

            print(to_df(ticks))
        elif i == 'e':
            print('bye bye.')
            break


threading.Thread(target=loop).start()
