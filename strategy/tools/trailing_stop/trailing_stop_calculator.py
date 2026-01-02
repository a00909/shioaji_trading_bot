from collections.abc import Callable


class TrailingStopCalculator:
    def __init__(self, volatility_getter: Callable):
        self._buffer = []
        self._is_long: bool | None = None
        self._volatility_getter = volatility_getter

    def reset(self):
        self._is_long = None
        self._buffer.clear()

    def _n_loss(self, multiplication, max_n_loss):
        n_loss = self._volatility_getter() * multiplication
        return min(max_n_loss, n_loss)

    @property
    def _latest(self):
        return self._buffer[-1] if len(self._buffer) > 0 else None

    def set_is_long(self, is_long: bool):
        self._is_long = is_long

    def calc_new_value(self, new_price, mult=3.5, max_loss=55):
        if self._is_long is None:
            return None

        if self._is_long:
            if self._latest:
                new = max(self._latest, new_price - self._n_loss(mult,max_loss))
            else:
                new = new_price - self._n_loss(mult,max_loss)
            self._buffer.append(new)
            return new
        else:
            if self._latest:
                new = min(self._latest, new_price + self._n_loss(mult,max_loss))
            else:
                new = new_price + self._n_loss(mult,max_loss)
            self._buffer.append(new)
            return new
