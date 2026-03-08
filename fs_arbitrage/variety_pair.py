from fs_arbitrage.variety_unit import VarietyUnit


class VarietyPair:
    def __init__(
            self,
            unit1: VarietyUnit,
            unit2: VarietyUnit,
            buy_multiplier: float,
            sell_multiplier: float,
            face_value: float
    ):
        self.buy_leg = unit1
        self.sell_leg = unit2
        self.buy_multiplier = buy_multiplier
        self.sell_multiplier = sell_multiplier
        self._face_value = face_value

    def identification(self):
        return f'{self.buy_leg.code}:{self.sell_leg.code}'

    def spread(self):
        b = self.sell_leg
        a = self.buy_leg

        return b.bid1 - a.ask1

    @property
    def buy_leg_price(self):
        return self.buy_leg.ask1

    @property
    def sell_leg_price(self):
        return self.sell_leg.bid1

    def cost(self):
        fixed_cost = (
                self.buy_leg.fixed_transaction_cost * self.buy_multiplier +
                self.sell_leg.fixed_transaction_cost * self.sell_multiplier
        )

        proportional_cost = (
                self.buy_leg_price *
                self.buy_leg.face_value *
                self.buy_leg.proportional_transaction_cost *
                self.buy_multiplier +
                self.sell_leg_price *
                self.sell_leg.face_value *
                self.sell_leg.proportional_transaction_cost *
                self.sell_multiplier
        )

        return fixed_cost + proportional_cost

    def proportional_profit(self):
        return (
                self.sell_leg_price * self.sell_multiplier * self.sell_leg.face_value -
                self.buy_leg_price * self.buy_multiplier * self.buy_leg.face_value
        )

    def profit_margin(self):
        return self.proportional_profit() - self.cost()

    def signal(self):
        return self.proportional_profit() > 0 #todo: test value
