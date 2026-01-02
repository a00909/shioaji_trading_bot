from datetime import datetime

from sqlalchemy import Column, Integer, Float, DateTime, Date, String, BigInteger, Boolean

from database.schema import Base

__all__ = ['KBar', 'KBarMemo']


from mixins.datetime_comparable_mixin import DatetimeComparableMixin

from tools.constants import DEFAULT_TIMEZONE


class KBar(Base, DatetimeComparableMixin):
    _datetime_comparable_field_name = 'ts'
    __tablename__ = 'k_bar'

    # 基礎表格定義，不會直接儲存資料
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String, index=True)
    ts = Column(DateTime(timezone=True), primary_key=True, index=True)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)
    amount = Column(Float, nullable=False)

    __table_args__ = (
        {
            "postgresql_partition_by": "RANGE(ts)",
        },
    )

    def to_string(self, separator: str = ":") -> str:
        return (
            f"{self.id}{separator}"
            f"{self.symbol}{separator}"
            f"{self.ts.timestamp()}{separator}"
            f"{self.open}{separator}"
            f"{self.high}{separator}"
            f"{self.low}{separator}"
            f"{self.close}{separator}"
            f"{self.volume}{separator}"
            f"{self.amount}"
        )

    def to_dict(self):
        return {
            'id': self.id,
            'symbol': self.symbol,
            'ts': self.ts,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'amount': self.amount,
        }

    @classmethod
    def from_string(cls, k_bar_string: bytes, separator: str = ":") -> 'KBar':
        """从字符串创建Tick对象"""
        parts = k_bar_string.decode('utf-8').split(separator)
        return cls(
            id=int(parts[0]),
            symbol=str(parts[1]),
            ts=datetime.fromtimestamp(float(parts[2])).replace(tzinfo=DEFAULT_TIMEZONE),
            open=float(parts[3]),
            high=float(parts[4]),
            low=float(parts[5]),
            close=float(parts[6]),
            volume=int(parts[7]),
            amount=float(parts[8])
        )


class KBarMemo(Base):
    __tablename__ = 'k_bar_memo'
    # 基礎表格定義，不會直接儲存資料
    date = Column(Date, primary_key=True)
    symbol = Column(String, primary_key=True)
