from pyflink.common import Row
from pyflink.datastream import ProcessFunction

from src.Conditions.default_condition import DefaultCondition
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class DuplicateTuple(StreamPolluter):
    """
    Duplicates a tuple.
    """
    def __init__(self, id_attribute: str):
        super().__init__('multiple tuples', id_attribute)
        self._condition = DefaultCondition()

    def _transform(self, value: Row) -> Row:
        value[self._id_attribute] = str(value[self._id_attribute]) + "_D"
        return value

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        yield value
        if self._condition.condition(value):
            yield self._transform(value)
