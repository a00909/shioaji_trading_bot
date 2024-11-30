from datetime import datetime

from sqlalchemy import Column, Integer, Float, DateTime, Date, String, BigInteger
from database.schema import Base

__all__ = ['HistoryTick', 'HistoryTickMemo']


class HistoryTick(Base):
    __tablename__ = 'history_tick'
    # 基礎表格定義，不會直接儲存資料
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    ts = Column(DateTime, primary_key=True, index=True)
    symbol = Column(String, index=True)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    bid_price = Column(Float)
    bid_volume = Column(Integer)
    ask_price = Column(Float)
    ask_volume = Column(Integer)
    tick_type = Column(Integer)

    __table_args__ = (
        {
            "postgresql_partition_by": "RANGE(ts)",
        },
    )

    def to_string(self, separator: str = ":") -> str:
        return (
            f"{self.ts.timestamp()}{separator}"
            f"{self.close}{separator}"
            f"{self.volume}{separator}"
            f"{self.bid_price}{separator}"
            f"{self.bid_volume}{separator}"
            f"{self.ask_price}{separator}"
            f"{self.ask_volume}{separator}"
            f"{self.tick_type}{separator}"
            f"{self.symbol}{separator}"
            f"{self.id}"
        )

    def to_dict(self):
        return {
            'id': self.id,
            'ts': self.ts,
            'symbol': self.symbol,
            'close': self.close,
            'volume': self.volume,
            'bid_price': self.bid_price,
            'bid_volume': self.bid_volume,
            'ask_price': self.ask_price,
            'ask_volume': self.ask_volume,
            'tick_type': self.tick_type
        }

    @classmethod
    def from_string(cls, tick_string: bytes, separator: str = ":") -> 'HistoryTick':
        """从字符串创建Tick对象"""
        parts = tick_string.decode('utf-8').split(separator)
        return cls(
            ts=datetime.fromtimestamp(float(parts[0])),
            close=float(parts[1]),
            volume=int(parts[2]),
            bid_price=float(parts[3]),
            bid_volume=int(parts[4]),
            ask_price=float(parts[5]),
            ask_volume=int(parts[6]),
            tick_type=int(parts[7]),
            symbol=str(parts[8]),
            id=int(parts[9])
        )


class HistoryTickMemo(Base):
    __tablename__ = 'history_tick_memo'
    # 基礎表格定義，不會直接儲存資料
    date = Column(Date, primary_key=True)
    symbol = Column(String, primary_key=True)
