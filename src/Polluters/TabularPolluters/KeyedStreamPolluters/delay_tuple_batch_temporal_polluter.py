import bisect

from pyflink.common import Types
from pyflink.datastream import RuntimeContext, KeyedProcessFunction
from pyflink.datastream.state import MapState, ListState, MapStateDescriptor, ListStateDescriptor

from src.Conditions.defaul_temporal_condition import DefaultTimeDependentPolluterCondition
from src.Conditions.default_condition import DefaultCondition
from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition
from src.Polluters.TabularPolluters.KeyedStreamPolluters.keyed_stream_polluter import KeyedStreamPolluter


class DelayTupleBatch(KeyedStreamPolluter):
    """
    In batch mode, Flink makes certain ordering guarantees for incoming data elements. Thus, this delay polluter
    checks the timestamp attribute of each incoming tuple. Tuples that should be delayed reside inside the keyed state
    until the timestamp of an incoming tuple triggers their release. For the case, none of the incoming tuples has a
    timestamp, which is equal or greater than the timestamp that triggers the release of the stored tuples, a timer is
    registered that causes the release of the stored tuples at the end of a task.
    """

    def __init__(self, d_type, id_attribute, delay_ms: int = 60000):  # 10 seconds
        super().__init__("temporal", id_attribute)
        self._delay_ms = delay_ms
        self.state: MapState = None
        self.state2: ListState = None
        self._condition = DefaultCondition()
        self._time_dependent_condition = DefaultTimeDependentPolluterCondition()
        self.d_type = d_type

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_map_state(
            MapStateDescriptor("my_state" + self.polluter_name, Types.LONG(), Types.LIST(self.d_type))
        )
        self.state2 = runtime_context.get_list_state(
            ListStateDescriptor("my_state2" + self.polluter_name, Types.LONG()))

    @property
    def time_dependent_condition(self) -> TimeDependentPolluterCondition:
        return self._time_dependent_condition

    def set_time_dependent_condition(self,
                                     time_dependent_condition: TimeDependentPolluterCondition) -> 'DelayTupleBatch':
        self._time_dependent_condition = time_dependent_condition
        return self

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        timer_service = ctx.timer_service()
        if not self.state2.get():
            self.state2.update([])

        idx = 0
        for ts in self.state2.get():
            if ts > ctx.timestamp():
                break

            current_state = self.state.get(ts)
            for val in current_state:
                yield val
            self.state.remove(ts)
            timer_service.delete_event_time_timer(ts)  # timer can be safely removed now
            idx += 1

        if idx > 0:  # Remove emitted values
            self.state2.update(list(self.state2.get())[idx:])

        if not self._condition.condition(value) or not self._time_dependent_condition.condition(ctx.timestamp()):
            yield value
            return

        fire_time = ctx.timestamp() + self._delay_ms

        if not self.state.contains(fire_time):
            self.state.put(fire_time, [])

        current_state = self.state.get(fire_time)
        current_state.append(value)
        self.state.put(fire_time, current_state)

        timer_service.register_event_time_timer(fire_time)

        # fire_times are saved to sorted list -> bisect inserts element at right position
        fire_times = list(self.state2.get())
        bisect.insort(fire_times, fire_time)
        self.state2.update(fire_times)

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        current_state = self.state.get(timestamp)
        for value in current_state:
            yield value
        self.state.remove(timestamp)
        self.state2.clear()
