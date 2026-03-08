from shioaji import Exchange, TickFOPv1, TickSTKv1, BidAskFOPv1, BidAskSTKv1, Shioaji

from fs_arbitrage.variety_unit import VarietyUnit


class UnitContainer:
    def __init__(self, units: list[VarietyUnit] = None):
        self._mapping: dict[str, VarietyUnit] = {}
        if units:
            self.extend(units)

    def add(self, unit: VarietyUnit):
        self._mapping[unit.code] = unit

    def extend(self, units: list[VarietyUnit]):
        for unit in units:
            self._mapping[unit.code] = unit

    def get(self, code):
        return self._mapping.get(code)

    def on_sj_data(self, _exchange: Exchange, sj_data: TickFOPv1 | TickSTKv1 | BidAskFOPv1 | BidAskSTKv1):
        unit = self.get(sj_data.code)

        if not unit:
            return

        unit.on_sj_data(sj_data)

    def get_all_tick_sub_data(self):
        data = []
        for unit in self._mapping.values():
            data.append(unit.get_tick_sub_data())
        return data

    def get_all_bidask_sub_data(self):
        data = []
        for unit in self._mapping.values():
            data.append(unit.get_bidask_sub_data())
        return data

    def subscribe_all(self, api: Shioaji):
        tick_sub_data = self.get_all_tick_sub_data()
        bidask_sub_data = self.get_all_bidask_sub_data()
        for t in tick_sub_data:
            api.quote.subscribe(**t)

        for b in bidask_sub_data:
            api.quote.subscribe(**b)

    def setup_callbacks(self, api: Shioaji):
        api.quote.set_on_tick_fop_v1_callback(self.on_sj_data)
        api.quote.set_on_tick_stk_v1_callback(self.on_sj_data)
        api.quote.set_on_bidask_fop_v1_callback(self.on_sj_data)
        api.quote.set_on_bidask_stk_v1_callback(self.on_sj_data)
