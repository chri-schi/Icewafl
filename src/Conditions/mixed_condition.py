import random
from typing import Annotated

from src.Conditions.polluter_condition import PolluterCondition
from src.Conditions.types.probability_type import ValueRange


class MixedCondition(PolluterCondition):
    """
    Combines a probability condition with a function condition.
    """

    def __init__(self, polluter_condition: PolluterCondition, probability: Annotated[float, ValueRange(0, 1)]):
        self._polluter_condition = polluter_condition
        self._probability = probability

    def condition(self, value) -> bool:
        return self._polluter_condition.condition(value) and random.random() < self._probability


