from enum import Enum


class QTOutputType(Enum):
    NONE = ''
    UPDATE_INDICATOR = 'update_indicator'
    APPEND_DEALS = 'append_deals'
    APPEND_SIGNALS = 'append_signals'