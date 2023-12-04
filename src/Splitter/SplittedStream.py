from pyflink.common import Row
from pyflink.datastream import DataStream

from src.Builder.polluter_builder import PolluterBuilderComposite, LogOutputManager
from src.Polluters.TabularPolluters.KeyedStreamPolluters.keyed_stream_polluter import KeyedStreamPolluter
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter
from src.Splitter.Splitter import Splitter


class SplittedStream:
    def __init__(self, datastreams: list[DataStream], stream_tags: list[str], id_attribute_name: str):
        self._tag_to_stream = {stream_tag: ds for stream_tag, ds in zip(stream_tags, datastreams)}
        self._tag_to_builder = {stream_tag: PolluterBuilderComposite() for stream_tag in stream_tags}
        self._tag_to_logger = {stream_tag: None for stream_tag in stream_tags}
        self._id_attribute_name = id_attribute_name

    @property
    def stream_tags(self):
        return list(self._tag_to_stream.keys())

    def get_stream(self, stream_tag: str):
        if stream_tag not in self._tag_to_stream:
            return None, False

        return self._tag_to_stream[stream_tag], True

    def apply_polluters(self, stream_tag: str, polluters: list[KeyedStreamPolluter, StreamPolluter]) -> int:
        if stream_tag not in self._tag_to_builder:
            return -1
        pb: PolluterBuilderComposite = self._tag_to_builder[stream_tag]
        pb.add_polluters(polluters)
        return len(pb)

    def apply_polluter(self, stream_tag: str, polluter: KeyedStreamPolluter | StreamPolluter) -> int:
        return self.apply_polluters(stream_tag, [polluter])

    def set_log_output_manager(self, stream_tag: str, log_output_manager: LogOutputManager):
        if stream_tag not in self._tag_to_builder:
            return False

        self._tag_to_logger[stream_tag] = log_output_manager
        return True

    def _tag_id_attribute(self, tag: str, id_attribute_name: str):
        def map_fct(value: Row) -> Row:
            tuple_id = value[id_attribute_name]
            value[id_attribute_name] = '_'.join([tuple_id, tag])
            return value

        return map_fct

    def merge_streams(self, tag_merged_tuples=False) -> DataStream:
        if tag_merged_tuples:
            for tag in self._tag_to_stream:
                self._tag_to_stream[tag] = self._tag_to_stream[tag].map(
                    self._tag_id_attribute(tag, self._id_attribute_name))

        for tag in self._tag_to_stream:
            self._tag_to_stream[tag] = self._tag_to_builder[tag].build(
                stream=self._tag_to_stream[tag],
                logger_handler=self._tag_to_logger[tag]
            )

        streams: list[DataStream] = list(self._tag_to_stream.values())
        ds: DataStream = streams.pop(0)
        return ds.union(*streams)

    @staticmethod
    def split_stream(splitter: Splitter, ds: DataStream, id_attribute_name: str) -> 'SplittedStream':
        output_tags = splitter.stream_tags
        output_tag_names = splitter.stream_tag_names
        ds = ds.process(splitter)
        side_outputs = [ds.get_side_output(output_tag) for output_tag in output_tags]
        return SplittedStream(side_outputs, output_tag_names, id_attribute_name)
