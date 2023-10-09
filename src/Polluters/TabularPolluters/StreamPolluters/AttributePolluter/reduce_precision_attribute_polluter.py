from typing import Any

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class ReducePrecision(AttributePolluter):
    """
    Rounds a numeric attribute.
    """

    def __init__(self, attribute: str, precision: int, id_attribute: str):
        self._attribute = attribute
        self._precision = precision
        super().__init__({attribute}, id_attribute)

    def _transform(self, value) -> Any:
        value[self._attribute] = round(value[self._attribute]) if self._precision == 0 else round(
            value[self._attribute], self._precision
        )
        return value
