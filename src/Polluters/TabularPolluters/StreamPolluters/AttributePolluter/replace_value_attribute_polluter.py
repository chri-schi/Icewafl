from typing import Any

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class ReplaceValue(AttributePolluter):
    """
    Replace the value of an attribute.
    """

    def __init__(self, attribute: str, id_attribute, replacement=None):
        self.replacement = replacement
        self._attribute = attribute
        super().__init__({attribute}, id_attribute)

    def _transform(self, value) -> Any:
        value[self._attribute] = self.replacement
        return value
