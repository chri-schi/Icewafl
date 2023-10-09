from pyflink.datastream import RuntimeContext, ProcessFunction

from src.Conditions.polluter_condition import PolluterCondition
from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition
from src.Logging.polluter_logger import OutputLogger
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class TimeDependentStreamPolluter(StreamPolluter):
    """
    Wrapper for StreamPolluters to allow the usage of event time dependent conditions.
    """

    def __init__(self, polluter: StreamPolluter, time_dependent_condition: TimeDependentPolluterCondition):
        super().__init__(polluter.polluted_element_type, polluter.id_attribute)
        self._time_dependent_condition = time_dependent_condition
        self._polluter = polluter

    def open(self, runtime_context: RuntimeContext):
        self._polluter.open(runtime_context)

    def close(self):
        self._polluter.close()

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        if self._time_dependent_condition.condition(ctx.timestamp()):
            for val in self._polluter.process_element(value, ctx):
                yield val
        else:
            yield value

    def set_condition(self, condition: PolluterCondition) -> 'TimeDependentStreamPolluter':
        self._polluter.set_condition(condition)
        return self

    @property
    def polluter_condition(self) -> PolluterCondition:
        return self._polluter.polluter_condition

    def set_polluter_name(self, polluter_name: str) -> 'TimeDependentStreamPolluter':
        self._polluter.set_polluter_name(polluter_name)
        return self

    @property
    def polluter_name(self) -> str:
        return self._polluter.polluter_name

    def set_output_logger(self, output_logger: OutputLogger) -> 'TimeDependentStreamPolluter':
        self._polluter.set_output_logger(output_logger)
        return self

    @property
    def is_logged(self):
        return self._polluter.is_logged

    @property
    def output_logger(self):
        return self._polluter.output_logger

    @property
    def polluted_element_type(self):
        return self._polluter.polluted_element_type

    @property
    def id_attribute(self):
        return self._polluter.id_attribute
