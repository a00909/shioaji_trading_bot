from django.utils.timezone import override

from strategy.tools.indicator_provider.extensions.data.bid_ask import BidAsk


class BidAskRatio(BidAsk):
    @override
    def _calc(self):
        return self.bid / (self.bid + self.ask) if (self.bid + self.ask) != 0 else 0
