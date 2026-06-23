from datetime import datetime, time

import numpy as np

from strategy.tools.kbar_indicators.intraday_interval_volume_avg.utils import minute_index


class IIVALookup:
    def __init__(self, data: np.ndarray | None, interval: int) -> None:
        self.data = data
        self.interval = interval

    def get(self, t: datetime | time):
        if None is self.data:
            return 0

        idx = minute_index(t, self.interval)
        return self.data[idx]
