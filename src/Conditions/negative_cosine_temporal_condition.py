import math
import random

from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition


class NegativeCosineCondition(TimeDependentPolluterCondition):
    """
    Used to simulate errors occurring periodically.

    a ... amplitude = 0.5
    b ... frequency
    c ... shift
    d ... offset = 0.5
    f(x) = a * cos(b * (x + c)) + d
    """

    def __init__(self, time_transformation, time_cycle_max_time: float, shift: float = 0.0):
        self._frequency = math.pi / (time_cycle_max_time * 0.5)
        self._shift = shift
        self._offset = 0.5
        self._amplitude = 0.5
        self._time_transformation = time_transformation
        self._noise_generator = lambda: 0.0

    @property
    def noise_generator(self):
        return self._noise_generator

    def set_noise_generator(self, noise_generator: float = lambda: random.gauss(mu=0, sigma=0.1)):
        self._noise_generator = noise_generator
        return self

    def condition(self, timestamp_ms: float) -> bool:
        rand_var = self._amplitude * math.cos(
            self._frequency * (self._time_transformation(timestamp_ms) + self._shift)
        ) + self._offset

        return random.random() < ((1 - rand_var) + self._noise_generator())
