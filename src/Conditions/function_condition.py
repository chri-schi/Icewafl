from typing import Callable

from src.Conditions.polluter_condition import PolluterCondition


class FunctionCondition(PolluterCondition):
    """
    Condition based on the attribute values of a tuple.
    """

    def __init__(self, condition_function: Callable[[any], bool]):
        self._condition_function = condition_function

    def condition(self, value) -> bool:
        try:
            return self._condition_function(value)
        except TypeError:
            return False
