import operator
from typing import Any

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class StringConcatAttributeValuesWithShift(AttributePolluter):
    """
    Concat the values of multiple string attributes inside a tuple.
    """
    def __init__(self, target_attribute: str, source_attributes: list[str], id_attribute: str, sep: str = " ",
                 null_value=None):
        super().__init__({target_attribute, *source_attributes}, id_attribute)
        self._target_attribute = target_attribute
        self._source_attributes = source_attributes
        self._sep = sep
        self._null_value = null_value

    def _attribute_shift_helper(self, value, attribute) -> str:
        try:
            return str(value[attribute])
        except TypeError:
            return ""
        finally:
            value[attribute] = self._null_value

    def _transform(self, value) -> Any:
        value[self._target_attribute] = self._sep.join(
            filter(
                operator.truth,
                [
                    self._attribute_shift_helper(value, self._target_attribute),
                    *[self._attribute_shift_helper(value, attribute) for attribute in self._source_attributes]
                ]
            )
        )
        return value
