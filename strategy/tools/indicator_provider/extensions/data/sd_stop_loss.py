from strategy.tools.indicator_provider.extensions.data.indicator import Indicator


class SdStopLoss(Indicator):
    def __init__(self):
        super().__init__()
        self.n_loss: float = None
        self.direction: int = 0
