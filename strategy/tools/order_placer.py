import logging
import threading
import typing

import shioaji as sj
from shioaji.constant import OrderState, Action
from shioaji.position import FuturePosition

from strategy.strategies.data import StrategySuggestion
from tools.dummy_shioaji import DummyShioaji

logger = logging.getLogger('Order Placer')


class OrderPlacer:
    def __init__(self, api: sj.Shioaji | DummyShioaji, contract, account, outer_cb=None):
        self.api: sj.Shioaji = api
        self.contract = contract
        self.completely_deal_event = threading.Event()
        self.account = account
        self.outer_cb: typing.Callable[[OrderState, dict], None] = outer_cb
        self.tmp_qty_counter = 0
        self.tmp_total_qty = 0
        self.tmp_last_deals = []

        self.api.set_order_callback(self.default_order_callback)

    def set_account(self, account):
        self.account = account

    def set_outer_callback(self, outer_cb: typing.Callable[[OrderState, dict], None]):
        self.outer_cb = outer_cb

    def default_order_callback(self, state: OrderState, msg: dict):
        logger.info(f'state: {state}, msg: {msg}')
        order_no = msg.get('ordno')

        if state in [OrderState.FuturesOrder, OrderState.StockOrder]:
            # self.__reset_counter(msg.get('quantity'))
            pass

        elif state in [OrderState.FuturesDeal, OrderState.StockDeal]:
            self.tmp_qty_counter += msg['quantity']
            self.tmp_last_deals.append(msg)

            logger.info(f'order {order_no} deal {self.tmp_qty_counter}/{self.tmp_total_qty}.')
            if self.tmp_qty_counter == self.tmp_total_qty:
                self.completely_deal_event.set()

        if self.outer_cb:
            self.outer_cb(state, msg)

    def wait_for_completely_deal(self):
        self.completely_deal_event.wait()
        self.completely_deal_event.clear()

    def get_last_deal_info(self):
        return self.tmp_last_deals

    def __reset_counter(self, total_qty):
        self.tmp_qty_counter = 0
        self.tmp_total_qty = total_qty
        self.tmp_last_deals.clear()
        self.completely_deal_event.clear()

    def close_all(self):
        # todo: maybe save the positions will be faster?

        positions: list[FuturePosition] = self.api.list_positions(self.account)

        if not positions:
            return None

        total_quantity = 0

        for p in positions:
            if p.direction == Action.Buy:
                total_quantity += p.quantity
            elif p.direction == Action.Sell:
                total_quantity -= p.quantity

        logger.info(f'{total_quantity} slots will be closed.')

        return self.place_order(-total_quantity)

    def get_default_order_data(self, qty, act: Action = None):
        if act:
            if qty <= 0:
                raise Exception('qty cannot <= zero!')
        else:
            if qty > 0:
                act = Action.Buy
            elif qty < 0:
                act = Action.Sell
            else:
                raise Exception('qty cannot be zero!')

        is_buy = act == Action.Buy

        od = {
            'price_type': sj.constant.FuturesPriceType.MKT,
            'order_type': sj.constant.OrderType.ROD,
            'octype': sj.constant.FuturesOCType.Auto,
            'account': self.account,
            'price': 0 if is_buy else 999999,
            'action': act,
            'quantity': abs(qty)
        }

        return self.api.Order(**od)

    def place_order(self, qty, act=None, cb=None):
        order = self.get_default_order_data(qty, act)
        self.__reset_counter(order.quantity)

        return self.api.place_order(self.contract, order, cb=cb)

    def simple_buy(self, qty=1, cb=None):
        return self.place_order(qty, cb=cb)

    def simple_sell(self, qty=1, cb=None):
        return self.place_order(-qty, cb=cb)

    def place_order_by_suggestion(self, ss: StrategySuggestion):
        return self.place_order(
            ss.quantity,
            ss.action
        )

    def list_positions(self):
        return self.api.list_positions(self.account)
