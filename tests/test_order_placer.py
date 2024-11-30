import threading
import time

from shioaji.constant import OrderState
from shioaji.order import Trade

from realtime.order_placer import OrderPlacer
from utils.app import App

app = App(init=True)
print(app.api.futopt_account)

order_placer = OrderPlacer(app.api, app.contract)

trading_finished = threading.Event()


def cb(stat:OrderState, msg:dict):
    print('order_cb:')
    print(f'stat: {stat}, msg: {msg}')
    if stat == OrderState.FuturesDeal:
        trading_finished.set()


app.api.set_order_callback(cb)
#
# trade = order_placer.simple_buy()
# print(f'trade: {trade}')
#
# time.sleep(0.5)
# trading_finished.wait()
# trading_finished.clear()
#
# app.api.update_status(
#     app.api.futopt_account,
#     trade
# )
# print('updated: ',trade)

# trade = order_placer.simple_sell(5)
# print(f'trade: {trade}')
#
# trading_finished.wait()
#
# print(app.api.list_profit_loss(app.api.futopt_account))



app.shut()
