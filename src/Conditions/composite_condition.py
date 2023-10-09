from functools import reduce

from src.Conditions.polluter_condition import PolluterCondition


class CompositeCondition(PolluterCondition):
    """
    Condition that harbours other (non-temporal) conditions.
    """

    def __init__(self, *polluter_conditions: [PolluterCondition]):
        self._polluter_conditions = polluter_conditions

    def condition(self, value) -> bool:
        return reduce(lambda c1, c2: c1.condition(value) and c2.condition(value), self._polluter_conditions)