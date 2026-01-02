from strategy.strategies.extensions.indicator_property_mixin import IndicatorPropertyMixin


class DonchianSwingStateMemorizer(IndicatorPropertyMixin):
    L_25 = 0
    H_25 = 1
    M = 2

    def __init__(self, indicator_facade):
        super().__init__(indicator_facade)
        self._states = []

    def get_state(self, idx=-1):
        n = len(self._states)
        if n == 0:
            return None
        if -n <= idx < n:
            return self._states[idx]
        return None

    def _add_state(self, state):
        self._states.append(state)

    def update(self):
        if self.get_state() != self.L_25 and self._donchian_l < self._ma_p < self._donchian_l_25:
            self._add_state(self.L_25)
        elif self.get_state() != self.H_25 and self._donchian_h_25 < self._ma_p < self._donchian_h:
            self._add_state(self.H_25)
        elif self.get_state() != self.M and self._donchian_l_25 < self._ma_p < self._donchian_h_25:
            self._add_state(self.M)


    def h25(self):
        return self.get_state() == self.H_25

    def down_cross_h25(self):
        return self.get_state(-2) == self.H_25 and self.get_state() == self.M

    def up_cross_h25(self):
        return self.get_state(-2) == self.M and self.get_state() == self.H_25

    def l25(self):
        return self.get_state() == self.L_25

    def up_cross_l25(self):
        return self.get_state(-2) == self.L_25 and self.get_state() == self.M

    def down_cross_l25(self):
        return self.get_state(-2) == self.M and self.get_state() == self.L_25
