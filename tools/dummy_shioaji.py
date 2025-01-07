from typing import Callable, List

from shioaji import Order
from shioaji.constant import OrderState, Action
from shioaji.position import FuturePosition, FutureProfitLoss

from tick_manager.history_tick_manager import HistoryTickManager
from tick_manager.rtm.dummy_rtm import DummyRealtimeTickManager


class DummyShioaji:
    class Quote:
        def __init__(self, parent):
            pass

        def set_on_tick_fop_v1_callback(self, callback: Callable[[str, object], None]):
            pass

        def set_on_bidask_fop_v1_callback(self, callback: Callable[[str, object], None]):
            pass

        def subscribe(self):
            pass

    def __init__(self, htm, dummy_rtm, account):
        self.foutopt_account = account
        self.long_positions: list[FuturePosition] = []
        self.short_positions: list[FuturePosition] = []
        self.order_callback = None
        self.quote = DummyShioaji.Quote(self)
        self.id_counter = 0
        self.htm: HistoryTickManager = htm
        self.dummy_rtm: DummyRealtimeTickManager = dummy_rtm
        self.price_per_point = 10
        self.profit_loss: list[FutureProfitLoss] = []
        self.Order = Order

    def list_positions(self, account: str) -> List[dict]:
        cur_price = self.dummy_rtm.latest_tick().close

        for p in self.long_positions:
            p.pnl = (cur_price - p.price) * p.quantity * self.price_per_point

        for p in self.short_positions:
            p.pnl = (p.price - cur_price) * p.quantity * self.price_per_point

        return self.long_positions + self.short_positions

    def list_profit_loss(self, account):
        return self.profit_loss

    def set_order_callback(self, callback: Callable[[str, dict], None]):
        self.order_callback = callback

    def _get_id(self, increase=True):
        ret = self.id_counter
        if increase:
            self.id_counter += 1
        return ret

    def place_order(self, account: str, order: Order, cb: Callable):
        if self.order_callback:
            # Simulate an order state callback
            order_state = OrderState.FuturesDeal
            oid = self._get_id()
            lt = self.latest_tick()
            # msg = {
            #     'operation': {'op_type': 'New', 'op_code': '00', 'op_msg': ''},
            #     'order': {
            #         'id': oid, 'seqno': oid, 'ordno': '000115',
            #         'account': {'account_type': 'F', 'broker_id': 'F002000', 'account_id': 'test000', 'signed': True},
            #         'action': order.action, 'price': order.price, 'quantity': order.quantity
            #     },
            #     'status': {'id': oid, 'exchange_ts': None,
            #                'order_quantity': order.quantity},
            #     'contract': {'security_type': 'FUT', 'exchange': 'TAIFEX', 'code': 'TMFL4'}
            # }
            msg = {'trade_id': oid, 'seqno': oid, 'ordno': oid, 'exchange_seq': oid,
                   'broker_id': 'F002000', 'account_id': 'test', 'action': order.action.name, 'code': 'TMFL4',
                   'price': lt.close,
                   'quantity': order.quantity, 'security_type': 'FUT', 'custom_field': '',
                   'ts': lt.datetime.timestamp()}

            self.handle_order(order, 'TMFTEST')

            self.order_callback(order_state, msg)

    def latest_tick(self):
        return self.dummy_rtm.latest_tick()

    def create_future_profit_loss(self, position, quantity, pnl, price):
        return FutureProfitLoss(
            id=position.id,
            code=position.code,
            quantity=quantity,
            pnl=pnl,
            date=self.latest_tick().datetime.isoformat(),
            entry_price=price,
            cover_price=price,
            tax=2,
            fee=15,
        )

    def create_future_position(self, code, direction, quantity, price):
        return FuturePosition(
            id=self._get_id(increase=False),
            code=code,
            direction=direction,
            quantity=quantity,
            price=price,
            last_price=price,
            pnl=0.0
        )

    def pnl(self, ent, cur, count, dpp=10, short=False):
        return count * (cur - ent) * dpp * -1 if short else 1

    def handle_order(self, order: Order, code):
        remaining_quantity = order.quantity
        latest_price = self.latest_tick().close

        if order.action == Action.Buy:
            # 沖銷 short positions
            while self.short_positions and remaining_quantity > 0:
                position = self.short_positions[0]
                if position.quantity <= remaining_quantity:
                    remaining_quantity -= position.quantity
                    pnl = self.pnl(position.price, latest_price, position.quantity, short=True)
                    self.profit_loss.append(
                        self.create_future_profit_loss(
                            position,
                            position.quantity,
                            pnl,
                            position.price
                        )
                    )
                    self.short_positions.pop(0)
                else:
                    position.quantity -= remaining_quantity
                    pnl = self.pnl(position.price, latest_price, position.quantity, short=True)
                    self.profit_loss.append(
                        self.create_future_profit_loss(
                            position,
                            remaining_quantity,
                            pnl,
                            position.price
                        )
                    )
                    remaining_quantity = 0

            # 如果還有剩餘數量，新增 long position
            if remaining_quantity > 0:
                self.long_positions.append(
                    self.create_future_position(code, Action.Buy, remaining_quantity, latest_price))

        elif order.action == Action.Sell:
            # 沖銷 long positions
            while self.long_positions and remaining_quantity > 0:
                position = self.long_positions[0]
                if position.quantity <= remaining_quantity:
                    remaining_quantity -= position.quantity
                    pnl = self.pnl(position.price, latest_price, position.quantity)
                    self.profit_loss.append(
                        self.create_future_profit_loss(
                            position,
                            position.quantity,
                            pnl,
                            position.price
                        )
                    )
                    self.long_positions.pop(0)
                else:
                    position.quantity -= remaining_quantity
                    pnl = self.pnl(position.price, latest_price, position.quantity)
                    self.profit_loss.append(
                        self.create_future_profit_loss(
                            position,
                            remaining_quantity,
                            pnl,
                            position.price
                        )
                    )
                    remaining_quantity = 0

            # 如果還有剩餘數量，新增 short position
            if remaining_quantity > 0:
                self.short_positions.append(
                    self.create_future_position(code, Action.Sell, remaining_quantity, latest_price)
                )

    def ticks(self):
        pass
