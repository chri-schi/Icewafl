from pyflink.datastream import ProcessFunction
from pyflink.datastream import DataStream, OutputTag
from src.Splitter.Splitter import Splitter


class StreamCopySplitter(Splitter):

    def __init__(self, n_streams, ds: DataStream):
        self._n_streams = n_streams
        self._stream_name = ds.get_name()
        self._stream_tag_names = ["_".join([self._stream_name, str(stream_id + 1)]) for stream_id in range(n_streams)]
        self._stream_tags = [OutputTag(stream_tag) for stream_tag in self._stream_tag_names]

    @property
    def stream_tags(self) -> list[OutputTag]:
        return self._stream_tags

    @property
    def stream_tag_names(self) -> list[str]:
        return self._stream_tag_names

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        for output_tag in self._stream_tags:
            yield output_tag, value
