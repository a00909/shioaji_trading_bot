import logging
from datetime import time

from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade

logger = logging.getLogger('ReversalStrategy')


class ReversalStrategy(AbsStrategy):
    """
    A momentum exhaustion reversal strategy based on the 'sell_buy_power' indicator.

    Core Logic:
    This strategy identifies moments when the market momentum is over-extended and likely to reverse.
    It enters a position not when the momentum indicator hits an extreme, but when it starts
    to recede from that extreme, providing a confirmation of reversal.

    Indicator:
    - `sell_buy_power`: A composite momentum indicator calculated as `sell_buy_ratio * volume_ratio`.
      A large positive value indicates high buying pressure on high volume, and vice-versa for negative values.

    Entry Conditions:
    - Short Entry: Enters a short position (`Sell`) when `sell_buy_power` first goes above
      a positive threshold (`sbp_threshold`) and then crosses back below it.
    - Long Entry: Enters a long position (`Buy`) when `sell_buy_power` first goes below
      a negative threshold (`-sbp_threshold`) and then crosses back above it.

    Exit Conditions:
    - Stop Loss: A dynamic stop loss is set based on a multiple of the standard deviation (`_sd`)
      at the time of entry.
    - Take Profit: The position is closed when the price reverts to a short-term moving average (`_pma_s`),
      capturing the profit from the mean-reversion.
    """

    def __init__(self,
                 indicator_facade: IndicatorFacade,
                 sbp_threshold: float = 2.5,
                 stop_loss_sd_multiplier: float = 2.0):
        """
        Initializes the ReversalStrategy.

        Args:
            indicator_facade: The facade to access market indicators.
            sbp_threshold: The threshold for `sell_buy_power` to be considered extreme.
            stop_loss_sd_multiplier: The multiplier for standard deviation to calculate stop loss.
        """
        super().__init__(
            indicator_facade,
            # Active trading time windows
            active_time_ranges=[
                (time(8, 50), time(13, 30)),
            ]
        )
        # --- Configurable Parameters ---
        self.sbp_threshold = sbp_threshold
        self.stop_loss_sd_multiplier = stop_loss_sd_multiplier

        # --- State Variables ---
        self.is_sbp_extreme_high = False  # Tracks if sbp has crossed the upper threshold
        self.is_sbp_extreme_low = False  # Tracks if sbp has crossed the lower threshold
        self.entry_sd = None  # Stores the SD value at the time of entry for stop-loss calculation

        logger.info(f"ReversalStrategy initialized with sbp_threshold={self.sbp_threshold} "
                    f"and stop_loss_sd_multiplier={self.stop_loss_sd_multiplier}")

    @property
    def name(self):
        return "ReversalStrategy"

    def _report_entry_detail(self, er: EntryReport):
        """A hook to report extra details upon entry. Can be implemented if needed."""
        pass

    def in_signal(self) -> StrategySuggestion | None:
        """Determines the entry signal for the strategy."""
        # Do not enter a new position if one is already open
        if self.er or not self._is_active_time:
            return None

        params = None
        sbp = self._net_buy_power

        # --- Short Entry Logic ---
        # 1. Detect extreme high momentum
        if sbp > self.sbp_threshold:
            self.is_sbp_extreme_high = True
            self.is_sbp_extreme_low = False

        # 2. Enter on reversal confirmation
        elif self.is_sbp_extreme_high and sbp < self.sbp_threshold:
            logger.info(f"[{self.indicator_facade.now()}] Short Entry Signal: sbp reverted from high. sbp={sbp:.2f}")
            params = [Action.Sell]
            self.entry_sd = self._sd  # Cache the SD at entry
            self.is_sbp_extreme_high = False

        # --- Long Entry Logic ---
        # 1. Detect extreme low momentum
        if sbp < -self.sbp_threshold:
            self.is_sbp_extreme_low = True
            self.is_sbp_extreme_high = False

        # 2. Enter on reversal confirmation
        elif self.is_sbp_extreme_low and sbp > -self.sbp_threshold:
            logger.info(f"[{self.indicator_facade.now()}] Long Entry Signal: sbp reverted from low. sbp={sbp:.2f}")
            params = [Action.Buy]
            self.entry_sd = self._sd  # Cache the SD at entry
            self.is_sbp_extreme_low = False

        return self._get_report(params)

    def out_signal(self) -> StrategySuggestion | None:
        """Determines the exit signal for the strategy."""
        if not self.er:
            return None

        params = None
        is_long = self.er.action == Action.Buy
        entry_price = self.er.deal_price
        current_price = self._ma_p  # Using a fast MA as the current price proxy

        # Ensure entry_sd was set
        if self.entry_sd is None:
            logger.warning("Exit signal called but entry_sd is not set. Using current _sd as fallback.")
            self.entry_sd = self._sd
            if self.entry_sd is None:  # If _sd is still None, cannot proceed
                return None

        stop_loss_level = self.entry_sd * self.stop_loss_sd_multiplier
        profit_target_ma = self._ma_s

        if is_long:
            # --- Long Position Exit Logic ---
            stop_loss_price = entry_price - stop_loss_level

            # 1. Stop Loss
            if current_price < stop_loss_price:
                logger.info(
                    f"[{self.indicator_facade.now()}] Long Stop Loss: price={current_price:.2f} < stop_price={stop_loss_price:.2f}")
                params = [Action.Sell]

            # 2. Take Profit
            elif current_price >= profit_target_ma:
                logger.info(
                    f"[{self.indicator_facade.now()}] Long Take Profit: price={current_price:.2f} >= ma={profit_target_ma:.2f}")
                params = [Action.Sell]
        else:
            # --- Short Position Exit Logic ---
            stop_loss_price = entry_price + stop_loss_level

            # 1. Stop Loss
            if current_price > stop_loss_price:
                logger.info(
                    f"[{self.indicator_facade.now()}] Short Stop Loss: price={current_price:.2f} > stop_price={stop_loss_price:.2f}")
                params = [Action.Buy]

            # 2. Take Profit
            elif current_price <= profit_target_ma:
                logger.info(
                    f"[{self.indicator_facade.now()}] Short Take Profit: price={current_price:.2f} <= ma={profit_target_ma:.2f}")
                params = [Action.Buy]

        # Reset states and cached values on exit
        if params:
            self.is_sbp_extreme_high = False
            self.is_sbp_extreme_low = False
            self.entry_sd = None

        return self._get_report(params, is_in=False)
