from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade


class IndicatorStateManager:
    def __init__(self,indicator_facade):
        self.indicator_facade:IndicatorFacade = indicator_facade
        self.state_log = []

