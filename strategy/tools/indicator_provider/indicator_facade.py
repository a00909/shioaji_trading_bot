from datetime import timedelta
from typing import Callable

from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider

__all__ = ["IndicatorFacade"]


class IndicatorFacade:
    def __init__(self, ip: IndicatorProvider):
        self._ip = ip

        self._unit = timedelta(minutes=1) #配合iiva須設定為1分鐘

        self.latest_price = IndicatorFacadeUnit(
            ip.latest_price,
            name='price'
        )

        # ma
        self._len_pma_p = timedelta(seconds=10)
        self.pma_p = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_pma_p),
            self._len_pma_p,
            IndicatorType.PMA
        )
        self._len_pma_s = timedelta(minutes=10.5)
        self.pma_s = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_pma_s),
            self._len_pma_s,
            IndicatorType.PMA
        )
        self._len_pma_m = timedelta(minutes=27.5)
        self.pma_m = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_pma_m),
            self._len_pma_m,
            IndicatorType.PMA
        )
        self._len_pma_l = timedelta(minutes=72)
        self.pma_l = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_pma_l),
            self._len_pma_l,
            IndicatorType.PMA
        )

        # volume
        self._vma_times = 2
        self._len_vma_l = timedelta(minutes=24)
        self.vma_l = IndicatorFacadeUnit(
            lambda: ip.vma(self._len_vma_l, self._unit, self._vma_times),
            self._len_vma_l,
            IndicatorType.VMA
        )
        self._len_vma_s = timedelta(minutes=5)
        self.vma_short = IndicatorFacadeUnit(
            lambda: ip.vma(self._len_vma_s, self._unit),
            self._len_vma_s,
            IndicatorType.VMA
        )
        # intraday interval value avg
        self._len_iiva = timedelta(days=30)
        self.iiva_l30d_i5m = IndicatorFacadeUnit(
            lambda: self._ip.intraday_interval_volume_avg(self._len_iiva, 420),
            name=f'iiva_l30d_i5m'
        )
        self.volume_ratio = IndicatorFacadeUnit(
            lambda: (self.vma_short() / self.iiva_l30d_i5m()),
            name='volume ratio'
        )

        # sell buy
        self._len_sell_buy_ratio = timedelta(minutes=5)
        self._len_sell_buy_ratio_change_rate = timedelta(minutes=3)
        self.sell_buy_ratio = IndicatorFacadeUnit(
            lambda: self._ip.sell_buy_ratio(self._len_sell_buy_ratio),
            self._len_sell_buy_ratio,
            IndicatorType.SELL_BUY_RATIO
        )
        self.sell_buy_ratio_change_rate = IndicatorFacadeUnit(
            lambda: self._ip.sell_buy_ratio_change_rate(self._len_sell_buy_ratio, self._len_sell_buy_ratio_change_rate),
            name=f'sell_buy_ratio_{self._len_sell_buy_ratio}_change_rate_{self._len_sell_buy_ratio_change_rate}'
        )
        self.sell_buy_power = IndicatorFacadeUnit(
            lambda: self.sell_buy_ratio() * self.volume_ratio(),
            name='sell_buy_power'
        )

        # sd
        self._bb_times = 2
        self._len_sd = timedelta(minutes=45)
        self.sd = IndicatorFacadeUnit(
            lambda: ip.standard_deviation(self._len_sd),
            self._len_sd,
            IndicatorType.SD
        )
        self.bb_upper = IndicatorFacadeUnit(
            lambda: (self.pma_l() + self.sd() * self._bb_times),
            name=f'bb_upper_*{self._bb_times}'
        )
        self.bb_lower = IndicatorFacadeUnit(
            lambda: (self.pma_l() - self.sd() * self._bb_times),
            name=f'bb_lower_*{self._bb_times}'
        )
        self.bb_width = IndicatorFacadeUnit(
            lambda: (self.sd() * self._bb_times),
            name=f'bb_width'
        )
        # self.sd_stop_loss = IndicatorFacadeUnit(
        #     lambda: self._ip.sd_stop_loss(
        #         self._len_sd,
        #         self._len_pma_p,
        #         self._len_vma_s,
        #         self._unit,
        #         self._len_sell_buy_ratio,
        #         self._len_sell_buy_ratio_change_rate,
        #         self._len_iiva
        #     ),
        #     self._len_sd,
        #     indicator_type=IndicatorType.SD_STOP_LOSS
        # )

        # covariance
        self._len_covariance_l = timedelta(hours=2)
        self.covariance_long = IndicatorFacadeUnit(
            lambda: self._ip.covariance(self._len_covariance_l),
            self._len_covariance_l,
            IndicatorType.COVARIANCE
        )
        self._len_covariance_s = timedelta(minutes=30)
        self.covariance_short = IndicatorFacadeUnit(
            lambda: self._ip.covariance(self._len_covariance_s),
            self._len_covariance_s,
            IndicatorType.COVARIANCE
        )

        # donchian
        self._len_donchian = timedelta(minutes=30)
        self.donchian_h = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).h,
            self._len_donchian,
            name='donchian_h'
        )
        self.donchian_l = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).l,
            self._len_donchian,
            name='donchian_l'
        )
        self.donchian_h_25 = IndicatorFacadeUnit(
            lambda: self.donchian_h() - (self.donchian_h() - self.donchian_l()) * 0.25,
            self._len_donchian,
            name='donchian_h_25'
        )
        self.donchian_l_25 = IndicatorFacadeUnit(
            lambda: self.donchian_l() + (self.donchian_h() - self.donchian_l()) * 0.25,
            self._len_donchian,
            name='donchian_l_25'
        )
        self.donchian_h_breakthrough = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).h_breakthrough,
            self._len_donchian,
            name='donchian_h_bk'
        )
        self.donchian_l_breakthrough = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).l_breakthrough,
            self._len_donchian,
            name='donchian_l_bk'
        )
        self.donchian_ll_accumulation = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).ll_accumulation,
            self._len_donchian,
            name='ll_accumulation'
        )
        self.donchian_hh_accumulation = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).hh_accumulation,
            self._len_donchian,
            name='hh_accumulation'
        )
        self.donchian_pivot_price = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).pivot_price,
            self._len_donchian,
            name='pivot_price'
        )
        self.donchian_pivot_price_changed = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).pivot_price_changed,
            self._len_donchian,
            name='pivot_price_changed'
        )
        self.donchian_pivot_price_serial = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).pivot_price_serial,
            self._len_donchian,
            name='pivot_price_serial'
        )
        self.donchian_idle = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian).idle_accumulation,
            self._len_donchian,
            name='donchian_idle'
        )


        self._len_donchian_s = timedelta(minutes=3)
        self.donchian_h_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).h,
            self._len_donchian_s,
            name='donchian_h_s'
        )
        self.donchian_l_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).l,
            self._len_donchian_s,
            name='donchian_l_s'
        )
        self.donchian_hh_accumulation_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).hh_accumulation,
            self._len_donchian_s,
            name='donchian_hh_s'
        )
        self.donchian_ll_accumulation_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).ll_accumulation,
            self._len_donchian_s,
            name='donchian_ll_s'
        )
        self.donchian_hl_accumulation_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).hl_accumulation,
            self._len_donchian_s,
            name='donchian_hl_s'
        )
        self.donchian_lh_accumulation_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).lh_accumulation,
            self._len_donchian_s,
            name='donchian_lh_s'
        )
        self.donchian_h_breakthrough_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).h_breakthrough,
            self._len_donchian_s,
            name='donchian_h_bk_s'
        )
        self.donchian_l_breakthrough_s = IndicatorFacadeUnit(
            lambda: self._ip.donchian(self._len_donchian_s).l_breakthrough,
            self._len_donchian_s,
            name='donchian_l_bk_s'
        )

        # self.bid_ask_diff = IndicatorFacadeUnit(
        #     lambda: self._ip.bid_ask_diff(),
        #     indicator_type=IndicatorType.BID_ASK_RATIO
        # )

        self.bid_ask_diff_ma = IndicatorFacadeUnit(
            lambda: self._ip.bid_ask_ratio(self._len_sell_buy_ratio),
            self._len_sell_buy_ratio,
            indicator_type=IndicatorType.BID_ASK_RATIO
        )



        # self.sd_stop_loss = IndicatorFacadeUnit(
        #     lambda: self._ip.sd_stop_loss(self._len_sd, self._len_very_short),
        #     self._len_sd,
        #     indicator_type=IndicatorType.SD_STOP_LOSS
        # )
        #
        # self.sd_stop_loss_short = IndicatorFacadeUnit(
        #     lambda: self._ip.sd_stop_loss(self._len_sd_short, self._len_very_short),
        #     self._len_sd_short,
        #     indicator_type=IndicatorType.SD_STOP_LOSS
        # )

        # # todo: 測試用，需完善
        # def dynamic_sd_stop_loss_f():
        #     if self.volume_ratio() <= 1:
        #         return self.sd_stop_loss_short()
        #     elif 1 < self.volume_ratio() <= 3:
        #         return self.sd_stop_loss()
        #     else:
        #         return self.sd_stop_loss_long()
        #
        # self.dynamic_sd_stop_loss = IndicatorFacadeUnit(
        #     lambda: dynamic_sd_stop_loss_f(),
        #     name='dynamic sd stop loss'
        # )

    def now(self):
        return self._ip.now


class IndicatorFacadeUnit:
    def __init__(self, getter, length=None, indicator_type=None, unit=None, name=None):
        self._indicator_type: IndicatorType = indicator_type
        self._getter: Callable = getter
        self._length: timedelta = length
        self._unit = unit
        self._name = name
        self._msg = None

    @property
    def name(self):
        if self._name:
            if self._length:
                return f'{self._name}_{self._length.total_seconds()}_s'
            else:
                return self._name
        else:
            if self._length:
                return f'{self._indicator_type.value}_{self._length.total_seconds()}_s'
            else:
                return self._indicator_type.value

    @property
    def msg(self):
        return f'{self.name}: {self()}'

    def __call__(self, *args, **kwargs):
        return self._getter(*args, **kwargs)
