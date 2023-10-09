from pyflink.common import Types
from pyflink.datastream import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import MapState, MapStateDescriptor

from src.Conditions.default_condition import DefaultCondition
from src.Conditions.polluter_condition import PolluterCondition
from src.Polluters.TabularPolluters.KeyedStreamPolluters.keyed_stream_polluter import KeyedStreamPolluter


class DelayTupleStream(KeyedStreamPolluter):
    """
    Tuples are intermediately stored in the keyed state. After a trigger based on event time fires the
    stored tuples are released from the keyed state. In this way it is possible to delay tuples.
    """

    def __init__(self, d_type, id_attribute: str, delay_ms: int = 60000):  # 10 seconds
        super().__init__("temporal", id_attribute)
        self._delay_ms = delay_ms
        self.state: MapState = None
        self._condition = DefaultCondition()
        self.d_type = d_type

    def set_condition(self, condition: PolluterCondition):
        self._condition = condition
        return self

    @property
    def condition(self):
        return self._condition

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_map_state(
            MapStateDescriptor("my_state", Types.LONG(), Types.LIST(self.d_type))
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        timer_service = ctx.timer_service()
        if not self._condition.condition(value):
            yield value
            return

        fire_time = ctx.timestamp() + self._delay_ms

        if not self.state.contains(fire_time):
            self.state.put(fire_time, [])

        current_state = self.state.get(fire_time)
        current_state.append(value)
        self.state.put(fire_time, current_state)

        timer_service.register_event_time_timer(fire_time)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        current_state = self.state.get(timestamp)
        for value in current_state:
            yield value
        self.state.remove(timestamp)
