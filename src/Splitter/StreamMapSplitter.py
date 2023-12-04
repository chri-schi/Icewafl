from typing import Iterable, Callable

from pyflink.datastream import ProcessFunction
from pyflink.datastream import DataStream, OutputTag
from src.Splitter.Splitter import Splitter


class StreamMapSplitter(Splitter):
    def __init__(
            self,
            tag_assignment_fct: Callable[[object], Iterable[str]],
            tag_assignment_to_stream_id: dict[object, str],
            ds: DataStream
    ):
        self._tag_assignment_fct = tag_assignment_fct
        self._tag_assignment_to_stream_id = tag_assignment_to_stream_id
        self._stream_name = ds.get_name()
        self._stream_tag_names = {
            tag_assignment: "_".join([self._stream_name, str(stream_id)])
            for tag_assignment, stream_id in self._tag_assignment_to_stream_id.items()
        }
        self._stream_tags = {
            tag_assignment: OutputTag(stream_tag) for tag_assignment, stream_tag in self._stream_tag_names.items()
        }

    def _assign_tag(self, value) -> Iterable[OutputTag]:
        for tag_assignment in self._tag_assignment_fct(value):
            if tag_assignment in self._tag_assignment_to_stream_id:
                yield self._stream_tags[tag_assignment]

    @property
    def stream_tags(self) -> list[OutputTag]:
        return list(self._stream_tags.values())

    @property
    def stream_tag_names(self) -> list[str]:
        return list(self._stream_tag_names.values())

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        for output_tag in self._assign_tag(value):
            yield output_tag, value
