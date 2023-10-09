import random
from typing import Any
from pyflink.common import Row

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class PermuteAttributes(AttributePolluter):
    """
    Permutes the attribute values of a tuple in a way that none of the attribute values is assigned to
    its source attribute.
    """

    def __init__(self, id_attribute: str, attributes: set[str]):
        super().__init__(attributes, id_attribute)
        self._attributes = attributes

    def shuffle(self, n: int):
        positions = list(range(n))
        for i in range(n - 1):
            rand_idx = random.randint(i + 1, n - 1)
            positions[i], positions[rand_idx] = positions[rand_idx], positions[i]
        return positions

    def _transform(self, value: Row) -> Any:
        attributes = [attribute for attribute in list(value.as_dict()) if attribute != self._id_attribute]

        if self._attributes is not None and len(self._attributes) > 0:
            attributes = [attribute for attribute in attributes if attribute in self._attributes]

        new_positions = self.shuffle(len(attributes))
        for idx, attribute in enumerate(attributes):
            value[attribute], value[attributes[new_positions[idx]]] = value[attributes[new_positions[idx]]], value[
                attribute]
        return value
