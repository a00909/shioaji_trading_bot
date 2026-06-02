from enum import StrEnum


class FeatureName(StrEnum):
    PRICE = 'price'
    NET_BUY_RATIO_S = 'net_buy_ratio_s'
    NET_BUY_RATIO_M = 'net_buy_ratio_m'
    NET_BUY_RATIO_L = 'net_buy_ratio_l'
    VOLUME_RATIO = 'volume_ratio'
    SD = 'sd'
    MOMENTUM_SHORT = 'momentum_short'
    MOMENTUM_LONG = 'momentum_long'
    BID_ASK_DIFF = 'bid_ask_diff'
    BID_ASK_IMBALANCE = 'bid_ask_imbalance'
    NET_BUY_RATIO_CHANGE = 'net_buy_ratio_change'
    NET_BUY_RATIO_REGIME = 'net_buy_ratio_regime'

    # dc
    DONCHIAN_HA = 'donchian_ha'
    DONCHIAN_LA = 'donchian_la'
    DONCHIAN_DIR = 'donchian_dir'
    DONCHIAN_H = 'donchian_h'
    DONCHIAN_L = 'donchian_l'
    DC_BRKOUT_ACCU = 'dc_brkout_accu'

    # label
    MAX_FAV = 'max_fav'
    MAX_ADV = 'max_adv'
    VALID_MASK = 'valid_mask'

    # time
    COS_TIME = 'cos_time'
    SIN_TIME = 'sin_time'
    IS_OP_30 = 'is_op_30'
    IS_CL_30 = 'is_cl_30'

    # directional sd
    DIR_SD = 'dir_sd'
