class Tick:
    ts: int
    close: float
    volume: int
    bid_price: float
    bid_volume: int
    ask_price: float
    ask_volume: int
    tick_type: int

    def __init__(self, ts: int, close: float, volume: int,
                 bid_price: float, bid_volume: int,
                 ask_price: float, ask_volume: int,
                 tick_type: int):
        self.ts = ts
        self.close = close
        self.volume = volume
        self.bid_price = bid_price
        self.bid_volume = bid_volume
        self.ask_price = ask_price
        self.ask_volume = ask_volume
        self.tick_type = tick_type

    def to_string(self, separator: str = ":") -> str:
        """将Tick对象转换为字符串，使用指定的分隔符"""
        return f"{self.ts}{separator}" \
               f"{self.close}{separator}" \
               f"{self.volume}{separator}" \
               f"{self.bid_price}{separator}" \
               f"{self.bid_volume}{separator}" \
               f"{self.ask_price}{separator}" \
               f"{self.ask_volume}{separator}" \
               f"{self.tick_type}"

    @classmethod
    def from_string(cls, tick_string: str, separator: str = ":") -> 'Tick':
        """从字符串创建Tick对象"""
        parts = tick_string.split(separator)
        return cls(
            ts=int(parts[0]),
            close=float(parts[1]),
            volume=int(parts[2]),
            bid_price=float(parts[3]),
            bid_volume=int(parts[4]),
            ask_price=float(parts[5]),
            ask_volume=int(parts[6]),
            tick_type=int(parts[7])
        )
