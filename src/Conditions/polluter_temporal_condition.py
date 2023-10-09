from abc import ABC, abstractmethod
from datetime import datetime


class TimeDependentPolluterCondition(ABC):
    """
    Abstract class for conditions that are time-dependent.
    """

    @abstractmethod
    def condition(self, timestamp_ms: float) -> bool:
        pass

    @staticmethod
    def extract_seconds(timestamp_ms: float) -> float:
        return datetime.fromtimestamp(timestamp_ms / 1000.0).second

    @staticmethod
    def extract_minutes(timestamp_ms: float) -> float:
        return datetime.fromtimestamp(timestamp_ms / 1000.0).minute

    @staticmethod
    def extract_hours(timestamp_ms: float) -> float:
        return datetime.fromtimestamp(timestamp_ms / 1000.0).hour
