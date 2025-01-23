from enum import Enum


class IndicatorType(Enum):
    PMA = 'pma'
    ATR = 'atr'
    VMA = 'vma'
    SD = 'sd'  # standard deviation

    @classmethod
    def from_string(cls, name: str):
        """Convert a string to an enum member."""
        try:
            return cls[name.upper()]  # Ensure case-insensitivity
        except KeyError:
            raise ValueError(f"'{name}' is not a valid name for {cls.__name__}")

    def to_string(self):
        """Convert an enum member to a string."""
        return self.name.lower()  # Convert to lowercase for consistency
