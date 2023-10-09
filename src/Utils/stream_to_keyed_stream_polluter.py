from pyflink.datastream import KeyedProcessFunction

from src.Polluters.TabularPolluters.KeyedStreamPolluters.keyed_stream_polluter import KeyedStreamPolluter
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class TemporalConverter(KeyedStreamPolluter):
    """
    Wrapper for Stream polluters to be used on keyed data streams.
    """
    def __init__(self, polluter: StreamPolluter):
        super().__init__(polluter.polluted_element_type, polluter.id_attribute)
        self._polluter = polluter

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if self._polluter.is_logged:  # logging not implemented for temporal polluters
            yield value

        for val in self._polluter.process_element(value, ctx):
            yield val

