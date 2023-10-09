from pyflink.datastream import RuntimeContext, ProcessFunction

from src.Conditions.defaul_temporal_condition import DefaultTimeDependentPolluterCondition
from src.Conditions.default_condition import DefaultCondition
from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class DurationPolluterBatch(StreamPolluter):
    """
    Assumes a sorted data stream, which should be the case in batch processing mode (no lateness).
    For stream processing mode consider using the DurationPolluter operating on keyed streams.
    """

    def __init__(self, polluter: StreamPolluter, duration_ms: int):
        super().__init__(polluter.polluted_element_type, polluter.id_attribute)
        self._polluter = polluter
        self._duration_ms = duration_ms
        self.fire_time = 0
        self._condition = DefaultCondition()
        self._time_dependent_condition = DefaultTimeDependentPolluterCondition()

    def open(self, runtime_context: RuntimeContext):
        self._polluter.open(runtime_context)

    def close(self):
        self._polluter.close()

    @property
    def time_dependent_condition(self) -> TimeDependentPolluterCondition:
        return self._time_dependent_condition

    def set_time_dependent_condition(self,
                                     time_dependent_condition: TimeDependentPolluterCondition) -> 'DurationPolluterBatch':
        self._time_dependent_condition = time_dependent_condition
        return self

    @property
    def polluter(self):
        return self._polluter

    def update_polluter(self, polluter: StreamPolluter):
        self._polluter = polluter
        return self

    @property
    def is_logged(self):
        return super().is_logged or self._polluter.is_logged

    def process_element(self, value, ctx: 'ProcessFunction.Context'):

        if ctx.timestamp() > self.fire_time and self._condition.condition(
                value) and self._time_dependent_condition.condition(ctx.timestamp()):
            fire_time = ctx.timestamp() + self._duration_ms
            self.fire_time = fire_time

        if ctx.timestamp() <= self.fire_time:
            for val in self._polluter.process_element(value, ctx):
                yield val
        else:
            yield value
