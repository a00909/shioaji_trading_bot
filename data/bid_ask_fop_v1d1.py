from decimal import Decimal

from shioaji import BidAskFOPv1

from data.datetime_comparable import DatetimeComparable
from tools.constants import DEFAULT_TIMEZONE


class BidAskFOPv1D1(BidAskFOPv1, DatetimeComparable):
    bid_price: float
    bid_volume: int
    ask_price: float
    ask_volume: int

    bid_price_org: list[Decimal]
    bid_volume_org: list[int]
    ask_price_org: list[Decimal]
    ask_volume_org: list[int]

    def __init__(self):
        DatetimeComparable.__init__(self, BidAskFOPv1D1)
        self.bid_volume_org = []

    @classmethod
    def bidaskv1_to_v1d1(cls, bidask: BidAskFOPv1):
        bidaskv1d1 = cls()
        bidaskv1d1.code = bidask.code
        bidaskv1d1.datetime = bidask.datetime.replace(tzinfo=DEFAULT_TIMEZONE)
        bidaskv1d1.bid_total_vol = bidask.bid_total_vol
        bidaskv1d1.ask_total_vol = bidask.ask_total_vol

        bidaskv1d1.bid_price_org = bidask.bid_price
        bidaskv1d1.bid_volume_org = bidask.bid_volume
        bidaskv1d1.ask_price_org = bidask.ask_price
        bidaskv1d1.ask_volume_org = bidask.ask_volume

        bidaskv1d1.bid_price = sum(bidask.bid_price)
        bidaskv1d1.bid_volume = sum(bidask.bid_volume)
        bidaskv1d1.ask_price = sum(bidask.ask_price)
        bidaskv1d1.ask_volume = sum(bidask.ask_volume)

        bidaskv1d1.diff_ask_vol = bidask.diff_ask_vol
        bidaskv1d1.diff_bid_vol = bidask.diff_bid_vol

        bidaskv1d1.first_derived_bid_price = bidask.first_derived_bid_price
        bidaskv1d1.first_derived_ask_price = bidask.first_derived_ask_price
        bidaskv1d1.first_derived_bid_vol = bidask.first_derived_bid_vol
        bidaskv1d1.first_derived_ask_vol = bidask.first_derived_ask_vol
        bidaskv1d1.underlying_price = bidask.underlying_price
        bidaskv1d1.simtrade = bidask.simtrade
        return bidaskv1d1
