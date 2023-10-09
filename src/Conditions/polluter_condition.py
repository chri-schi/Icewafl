from abc import ABC, abstractmethod


class PolluterCondition(ABC):
    """
    Interface for conditions based on attribute values or random variables. Generally, all conditions that are
    not time-dependent implement this interface.
    """

    @abstractmethod
    def condition(self, value) -> bool:
        pass
