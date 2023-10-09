from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapState, MapStateDescriptor


class KeyedStreamSorter(KeyedProcessFunction):
    def __init__(self, d_type):
        self.state: MapState = None
        self._d_type = d_type

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_map_state(
            MapStateDescriptor("my_state", Types.LONG(), Types.LIST(self._d_type))
        )

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        timer_service = ctx.timer_service()
        fire_time = ctx.timestamp()

        if fire_time > timer_service.current_watermark():
            if not self.state.contains(fire_time):
                self.state.put(fire_time, [])

            current_state = self.state.get(fire_time)
            current_state.append(value)
            self.state.put(fire_time, current_state)

            timer_service.register_event_time_timer(fire_time)
        else:  # drop late events
            pass

    def on_timer(self, timestamp: int, ctx: 'KeyedProcessFunction.OnTimerContext'):
        current_state = self.state.get(timestamp)
        for value in current_state:
            yield value
        self.state.remove(timestamp)
