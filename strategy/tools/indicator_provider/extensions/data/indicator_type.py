from enum import Enum


class IndicatorType(Enum):
    PMA = 'pma'
    ATR = 'atr'
    VMA = 'vma'
    SD = 'sd'  # standard deviation
    COVARIANCE = 'covariance'
    SELL_BUY_DIFF = 'sell_buy_diff'
    BID_ASK_DIFF = 'bid_ask_diff'
    BID_ASK_RATIO_MA = 'bid_ask_ratio_ma'
    SD_STOP_LOSS = 'sd_stop_loss'

    @classmethod
    def from_string(cls, name: str):
        """Convert a string to an enum member."""
        try:
            return cls[name.upper()]  # Ensure case-insensitivity
        except KeyError:
            raise ValueError(f"'{name}' is not a valid name for {cls.__name__}")

    def to_string(self):
        """Convert an enum member to a string."""
        return self.name.lower()  # Convert to lowercase for consistency
