from abc import abstractmethod, ABC
from pyflink.common import Types, TypeInformation
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.datastream.connectors.file_system import FileSource


class PythonToFlinkTypeMapper(ABC):
    @abstractmethod
    def map(self, python_type) -> TypeInformation:
        pass


class WriterBase(ABC):
    @abstractmethod
    def write(self, data_stream: DataStream):
        pass


class ReaderBase(ABC):
    def __init__(self, filepath: str, data_schema: dict[str:str], type_mapper: PythonToFlinkTypeMapper,
                 id_attribute: str = 'myId'):
        self._filepath = filepath
        self._data_schema = data_schema
        self._id_attribute = id_attribute
        self._ts_assigner = None
        self._type_mapper = type_mapper

    @property
    def ts_assigner(self):
        return self._ts_assigner

    def set_ts_assigner(self, ts_assigner: TimestampAssigner):
        self._ts_assigner = ts_assigner
        return self

    @property
    def _attribute_names(self):
        for attribute_name in self._data_schema:
            yield attribute_name

    @property
    def attribute_names(self):
        yield self._id_attribute
        for attribute_name in self._attribute_names:
            yield attribute_name

    @property
    def _attribute_data_types(self):
        for attribute_name in self._attribute_names:
            yield self._type_mapper.map(self._data_schema[attribute_name])

    @property
    def attribute_data_types(self):
        yield Types.STRING()
        for attribute_data_type in self._attribute_data_types:
            yield attribute_data_type

    def _create_tuple_type(self):
        return Types.ROW_NAMED(
            list(self._attribute_names),
            list(self._attribute_data_types)
        )

    @property
    def tuple_type(self):
        return Types.ROW_NAMED(
            list(self.attribute_names),
            list(self.attribute_data_types)
        )

    @abstractmethod
    def _init_source(self) -> FileSource:
        pass

    @abstractmethod
    def _get_stream_from_source(self, env: StreamExecutionEnvironment, source_name: str) -> DataStream:
        pass

    @abstractmethod
    def _assign_id(self, data_stream: DataStream) -> DataStream:
        pass

    @abstractmethod
    def _assign_timestamp(self, data_stream: DataStream) -> DataStream:
        pass

    def get_stream(self, env: StreamExecutionEnvironment, source_name: str) -> DataStream:
        ds = self._get_stream_from_source(env, source_name)
        ds = self._assign_id(ds)
        ds = self._assign_timestamp(ds)
        return ds

    @abstractmethod
    def _get_stream_with_ground_truth(self, data_stream: DataStream) -> tuple[DataStream, DataStream]:
        pass

    def get_stream_with_ground_truth(self, env: StreamExecutionEnvironment, source_name: str) -> tuple[
        DataStream, DataStream]:
        return self._get_stream_with_ground_truth(self.get_stream(env, source_name))
