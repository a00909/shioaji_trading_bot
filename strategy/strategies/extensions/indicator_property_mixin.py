from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade


class IndicatorPropertyMixin:
    def __init__(self, indicator_facade):
        self.indicator_facade: IndicatorFacade = indicator_facade

    @property
    def _ma_p(self):
        return self.indicator_facade.pma_p()

    @property
    def _ma_s(self):
        return self.indicator_facade.pma_s()

    @property
    def _ma_m(self):
        return self.indicator_facade.pma_m()

    @property
    def _ma_l(self):
        return self.indicator_facade.pma_l()

    @property
    def _price(self):
        return self.indicator_facade.latest_price()

    @property
    def _sd(self):
        return self.indicator_facade.sd()

    @property
    def _vma_long(self):
        return self.indicator_facade.vma_l()

    @property
    def _vma_short(self):
        return self.indicator_facade.vma_short()

    @property
    def _covariance_long(self):
        return self.indicator_facade.covariance_long()

    @property
    def _covariance_short(self):
        return self.indicator_facade.covariance_short()

    @property
    def _is_high_volume(self):
        return self._vma_short >= self._vma_long

    @property
    def _sell_buy_ratio(self):
        return self.indicator_facade.sell_buy_ratio()

    @property
    def _sell_buy_power(self):
        return self.indicator_facade.sell_buy_power()

    @property
    def _sell_buy_ratio_change_rate(self):
        return self.indicator_facade.sell_buy_ratio_change_rate()

    @property
    def _bid_ask_ratio(self):
        return self.indicator_facade.bid_ask_diff_ma()

    @property
    def _sd_stop_loss(self):
        return self.indicator_facade.sd_stop_loss()

    @property
    def _iiva(self):
        return self.indicator_facade.iiva_l30d_i5m()

    @property
    def _volume_ratio(self):
        return self.indicator_facade.volume_ratio()

    @property
    def _bb_lower(self):
        return self.indicator_facade.bb_lower()

    @property
    def _bb_upper(self):
        return self.indicator_facade.bb_upper()

    @property
    def _bb_width(self):
        return self.indicator_facade.bb_width()

    @property
    def _donchian_h(self):
        return self.indicator_facade.donchian_h()

    @property
    def _donchian_l(self):
        return self.indicator_facade.donchian_l()

    @property
    def _donchian_width(self):
        return self._donchian_h - self._donchian_l

    @property
    def _donchian_breakthrough_h(self):
        return self.indicator_facade.donchian_h_breakthrough()

    @property
    def _donchian_breakthrough_l(self):
        return self.indicator_facade.donchian_l_breakthrough()

    @property
    def _donchian_h_25(self):
        return self._donchian_h - (self._donchian_h - self._donchian_l) * 0.25

    @property
    def _donchian_l_25(self):
        return self._donchian_l + (self._donchian_h - self._donchian_l) * 0.25

    @property
    def _donchian_50(self):
        return (self._donchian_l + self._donchian_h) * 0.5

    @property
    def _donchian_hh_accumulation(self):
        return self.indicator_facade.donchian_hh_accumulation()

    @property
    def _donchian_ll_accumulation(self):
        return self.indicator_facade.donchian_ll_accumulation()

    @property
    def _donchian_pivot_price(self):
        return self.indicator_facade.donchian_pivot_price()

    @property
    def _donchian_hh_accumulation_s(self):
        return self.indicator_facade.donchian_hh_accumulation_s()

    @property
    def _donchian_ll_accumulation_s(self):
        return self.indicator_facade.donchian_ll_accumulation_s()

    @property
    def _donchian_idle(self):
        return self.indicator_facade.donchian_idle()

    @property
    def _donchian_lh_accumulation_s(self):
        return self.indicator_facade.donchian_lh_accumulation_s()

    @property
    def _donchian_hl_accumulation_s(self):
        return self.indicator_facade.donchian_hl_accumulation_s()

    @property
    def _donchian_breakthrough_h_s(self):
        return self.indicator_facade.donchian_h_breakthrough_s()

    @property
    def _donchian_breakthrough_l_s(self):
        return self.indicator_facade.donchian_l_breakthrough_s()
