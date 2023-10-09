import random
from typing import Any

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class GaussianNoise(AttributePolluter):
    """
    Adds additive gaussian noise to a numeric attribute.
    """
    def __init__(self, attribute: str, id_attribute: str, mu: float = 0, sigma: float = 0.2, ):
        self._attribute = attribute
        self._mu = mu
        self._sigma = sigma
        super().__init__({attribute}, id_attribute)

    def _transform(self, value) -> Any:
        value[self._attribute] = value[self._attribute] + random.gauss(mu=self._mu, sigma=self._sigma)
        return value
