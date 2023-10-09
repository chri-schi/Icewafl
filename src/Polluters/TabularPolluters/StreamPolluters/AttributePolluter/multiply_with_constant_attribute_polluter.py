from typing import Any

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter


class MultiplyWithConstant(AttributePolluter):
    """
    Multiplies a numeric attribute with a constant value.
    """

    def __init__(self, attributes: list[str], multiplier: list[float], id_attribute: str):
        self._attributes_ = attributes
        self._multiplier = multiplier
        super().__init__(set(attributes), id_attribute)

    def _transform(self, value) -> Any:
        for attribute, multiplier in zip(self._attributes_, self._multiplier):
            value[attribute] = value[attribute] * multiplier

        return value
