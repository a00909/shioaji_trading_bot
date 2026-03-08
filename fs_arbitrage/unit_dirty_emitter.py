from typing import Protocol

class UnitDirtyEmitter(Protocol):
    def __call__(self, unit_id: str) -> None: ...