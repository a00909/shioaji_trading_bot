import shioaji as sj
from shioaji.position import FuturePosition


class OrderPlacer:
    def __init__(self, api: sj.Shioaji, contract):
        self.api: sj.Shioaji = api
        self.contract = contract

    def simple_buy(self, qty=1, cb=None):
        order = self.api.Order(
            action=sj.constant.Action.Buy,
            price=0,
            quantity=qty,
            price_type=sj.constant.FuturesPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,
            octype=sj.constant.FuturesOCType.Auto,
            account=self.api.futopt_account
        )
        return self.api.place_order(self.contract, order, cb=cb)

    def close_all(self):
        # todo: maybe save the positions will be faster?
        positions: list[FuturePosition] = self.api.list_positions()
        total_quantity = sum(*[p.quantity for p in positions])
        return self.simple_sell(total_quantity)

    def simple_sell(self, qty=1, cb=None):
        order = self.api.Order(
            action=sj.constant.Action.Sell,
            price=999999,
            quantity=qty,
            price_type=sj.constant.FuturesPriceType.MKT,
            order_type=sj.constant.OrderType.ROD,
            octype=sj.constant.FuturesOCType.Auto,
            account=self.api.futopt_account
        )
        return self.api.place_order(self.contract, order, cb=cb)
