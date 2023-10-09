from functools import reduce

from pyflink.common import Row
from pyflink.datastream import RuntimeContext, OutputTag

from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.attribute_polluter import AttributePolluter
from typing import Any


class CompositeAttributePolluter(AttributePolluter):
    """
    Composite attribute polluters can harbour an arbitrary number of attribute polluters that are than applied
    together during the pollution process. Because composite attribute polluters are attribute polluters themselves,
    they can also harbour other composite attribute polluters and thus allow the definition of a nested pollution
    processes.
    """

    def __init__(self, attribute_polluters: list[AttributePolluter], id_attribute: str):
        super().__init__(reduce(lambda a, b: a.union(b), [x.polluted_attributes for x in attribute_polluters]),
                         id_attribute)
        self._attribute_polluters = attribute_polluters

    @property
    def polluter_name(self):
        return type(self).__name__ + ': [' + " ".join(
            [attribute_polluter.polluter_name for attribute_polluter in self._attribute_polluters]
        ) + ']'

    def open(self, runtime_context: RuntimeContext):
        for attribute_polluter in self._attribute_polluters:
            attribute_polluter.open(runtime_context)

    def close(self):
        for attribute_polluter in self._attribute_polluters:
            attribute_polluter.close()

    def _transform(self, value) -> Any:
        return self.transform(value)[0]

    def transform(self, value) -> tuple[Any, list[tuple[OutputTag, Row]]]:
        collected_side_output_values = []
        for attribute_polluter in self._attribute_polluters:
            attribute_polluter.set_output_logger(self.output_logger)
            if attribute_polluter.polluter_condition.condition(value):
                value, side_output_values = attribute_polluter.transform(value)
                collected_side_output_values += side_output_values
        return value, collected_side_output_values
