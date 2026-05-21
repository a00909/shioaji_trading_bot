from typing import Protocol

from sqlalchemy import Column


class MemoProtocol(Protocol):
    date: Column
    symbol: Column