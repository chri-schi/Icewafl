from typing import Any

from src.Conditions.composite_condition import CompositeCondition
from src.Conditions.function_condition import FunctionCondition
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class TransformValues(AttributePolluter):
    """
    Apply a user defined function on data tuples.
    """

    def __init__(self, attribute, transformation, expected_data_types: dict, id_attribute: str):
        self._attribute = attribute
        self._transformation = transformation
        super().__init__({attribute}, id_attribute)
        self._init_condition(expected_data_types)

    def _transform(self, value) -> Any:
        value[self._attribute] = self._transformation(value)

    def _init_condition(self, expected_data_types: dict):
        def condition(value) -> bool:
            try:
                for attribute in expected_data_types:
                    expected_data_types[attribute](value[attribute])
            except ValueError:
                return False
            except TypeError:
                return False
            return True

        self._condition = CompositeCondition(self._condition, FunctionCondition(condition))
