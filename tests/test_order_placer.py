import logging
import threading
import time

from shioaji.constant import OrderState

from strategy.tools.order_placer import OrderPlacer
from tools.app import App

# logging.basicConfig(
#     level=logging.INFO,  # 设置日志级别
#     # format='%(asctime)s - %(levelname)s - %(message)s'  # 设置输出格式
# )


def cb(stat: OrderState, msg: dict):
    print('[outer callback]')
    print(f'stat: {stat}, msg: {msg.get("ts")}')


app = App(init=True)
print(app.api.futopt_account)

order_placer = OrderPlacer(app.api, app.contract, app.api.futopt_account)

trade = order_placer.simple_buy(8)
order_placer.wait_for_completely_deal()

print('positions:', app.api.list_positions(app.api.futopt_account),'\n')
print('profit_loss:', app.api.list_profit_loss(app.api.futopt_account),'\n')
# print('trades:', app.api.list_trades(),'\n')

trade = order_placer.close_all()
order_placer.wait_for_completely_deal()

print('positions:', app.api.list_positions(app.api.futopt_account),'\n')
print('profit_loss:', app.api.list_profit_loss(app.api.futopt_account),'\n')
# print('trades:', app.api.list_trades(),'\n')

app.shut()
