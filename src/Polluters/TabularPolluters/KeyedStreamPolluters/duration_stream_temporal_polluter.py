from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueState, ListState, ValueStateDescriptor, ListStateDescriptor

from src.Conditions.default_condition import DefaultCondition
from src.Conditions.polluter_condition import PolluterCondition
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter


class DurationPolluterStream(KeyedProcessFunction):
    """
    Applies a data error for a certain duration. When applied, it is not possible to re-apply the polluter before the
    defined duration ended.
    """

    def __init__(self, polluter: StreamPolluter, duration_ms: int, d_type):
        self._polluter = polluter
        self._duration_ms = duration_ms
        self._condition = DefaultCondition()
        self.state_begin: ValueState = None
        self.state_end: ValueState = None
        self.state_values: ListState = None
        self._d_type = d_type

    def set_condition(self, condition: PolluterCondition):
        self._condition = condition
        return self

    @property
    def condition(self):
        return self._condition

    def open(self, runtime_context: RuntimeContext):
        self._polluter.open(runtime_context)
        self.state_begin = runtime_context.get_state(ValueStateDescriptor('state_begin', Types.LONG()))
        self.state_end = runtime_context.get_state(ValueStateDescriptor('state_end', Types.LONG()))
        self.state_values = runtime_context.get_list_state(ListStateDescriptor('state_end', self._d_type))

    def close(self):
        self._polluter.close()

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        if self.state_begin.value() is None:
            self.state_begin.update(0)

        if self.state_end.value() is None:
            self.state_begin.update(0)

        fire_time = self.state_end.value()

        if fire_time <= 0 and self._condition.condition(value):
            fire_time = ctx.timestamp() + self._duration_ms
            ctx.timer_service().register_event_time_timer(fire_time)
            self.state_end.update(fire_time)
            self.state_begin.update(ctx.timestamp())

        if self.state_end.value() >= ctx.timestamp() >= self.state_begin.value():
            if self.state_values.get() is None:
                self.state_values.update([])
            self.state_values.add(value)
        else:
            yield value

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        for value in self.state_values.get():
            for val in self._polluter.process_element(value, ctx):
                yield val
        self.state_values.clear()
        self.state_begin.clear()
        self.state_end.clear()