import random
from typing import Any

from pyflink.common import Row

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class SemiEmptyTuple(AttributePolluter):
    """
    Define the minimum number of attribute values being null inside a tuple.
    """

    def __init__(self, id_attribute: str, missing_value_proportion: float, null_value=None):
        super().__init__(set(), id_attribute)
        self._missing_value_proportion = missing_value_proportion
        self._null_value = null_value

    def _transform(self, value: Row) -> Any:
        attributes = [attribute for attribute in list(value.as_dict()) if attribute != self._id_attribute]
        n_missing = min(int(len(attributes) * self._missing_value_proportion), len(attributes))

        if n_missing <= 0:
            self._attributes = set()
            return value

        not_null_attributes = [attribute for attribute in attributes if value[attribute] != self._null_value]
        n_not_null_attributes = len(not_null_attributes)
        n_null_attributes = len(attributes) - n_not_null_attributes

        if n_missing <= n_null_attributes:  # Tuple is already semi empty
            self._attributes = set()
            return value

        n_missing -= n_null_attributes  # Subtract already included null values

        attributes = set(random.sample(not_null_attributes, n_missing))
        for attribute in attributes:
            value[attribute] = self._null_value

        self._attributes = set(attributes)
        return value
