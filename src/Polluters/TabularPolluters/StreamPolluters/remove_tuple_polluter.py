from pyflink.datastream import ProcessFunction

from src.Conditions.default_condition import DefaultCondition
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class RemoveTuple(StreamPolluter):
    """
    Removes tuples.
    """
    def __init__(self, id_attribute: str):
        super().__init__('schema', id_attribute)
        self._condition = DefaultCondition()

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        if self._condition.condition(value):
            return
        else:
            yield value
