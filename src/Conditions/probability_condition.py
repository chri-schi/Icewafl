import random
from typing import Annotated

from src.Conditions.polluter_condition import PolluterCondition
from src.Conditions.types.probability_type import ValueRange


class ProbabilityCondition(PolluterCondition):
    """
    Condition based on a value drawn from a uniform distribution.
    """

    def __init__(self, probability: Annotated[float, ValueRange(0, 1)]):
        self._probability = probability

    def condition(self, value) -> bool:
        return random.random() < self._probability
