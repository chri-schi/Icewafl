import json

from pyflink.common import Types, WatermarkStrategy, Row, Encoder
from pyflink.datastream import StreamExecutionEnvironment, DataStream, OutputTag
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, \
    RollingPolicy
from pyflink.datastream.formats.csv import CsvSchema, CsvBulkWriters

from src.ReadWrite.CSV.read_write_utils import CsvPythonToFlinkTypeMapper, CreateRow, ParseData, AssignId, WriteToSideOutput
from src.ReadWrite.reader_writer_base import ReaderBase, WriterBase


class CsvWriterSimple(WriterBase):
    def __init__(self, output_directory: str, separator: str = ","):
        self._output_directory = output_directory
        self._separator = separator

    def to_csv_row(self, value: Row) -> str:
        return self._separator.join([str(attribute) for attribute in value.as_dict().values()])

    def write(self, data_stream: DataStream):
        output_config = OutputFileConfig.builder() \
            .with_part_prefix("prefix") \
            .with_part_suffix(".csv") \
            .build()

        fs = FileSink.for_row_format(base_path=self._output_directory, encoder=Encoder.simple_string_encoder()) \
            .with_output_file_config(output_config) \
            .with_rolling_policy(RollingPolicy.default_rolling_policy()) \
            .build()

        data_stream.map(self.to_csv_row, output_type=Types.STRING())

        return data_stream.sink_to(fs)


class CsvWriter(WriterBase):
    def __init__(self, output_directory: str, attribute_names: list[str], seperator: str = ','):
        self._output_directory = output_directory
        self._attribute_names = attribute_names
        self._seperator = seperator

    def value_to_string(self, value: Row) -> Row:
        for attribute in value.as_dict():
            value[attribute] = str(value[attribute])
        return value

    def write(self, data_stream: DataStream):
        csv_schema = CsvSchema.builder()
        for attribute in self._attribute_names:
            csv_schema = csv_schema.add_string_column(attribute)

        csv_schema = csv_schema.set_column_separator(self._seperator).build()

        fs = FileSink.for_bulk_format(self._output_directory, CsvBulkWriters.for_schema(csv_schema)).build()
        data_stream = data_stream.map(
            self.value_to_string,
            output_type=Types.ROW_NAMED(self._attribute_names, [Types.STRING()]*len(self._attribute_names))
        ).name("to string")
        return data_stream.sink_to(fs)


class CsvReader(ReaderBase):
    def __init__(self, filepath: str, path_to_data_schema: str, seperator: str = ',', id_attribute: str = 'myId'):
        f = open(path_to_data_schema)
        data_schema = json.load(f)
        f.close()

        super().__init__(filepath, data_schema, CsvPythonToFlinkTypeMapper(), id_attribute)
        self._seperator = seperator
        self._source = self._init_source()
        self._tuple_type = self._create_tuple_type()

    def _init_source(self) -> FileSource:
        return FileSource \
            .for_record_stream_format(StreamFormat.text_line_format(), self._filepath) \
            .build()

    def _get_stream_from_source(self, env: StreamExecutionEnvironment, source_name: str) -> DataStream:
        return env.from_source(
            self._source,
            WatermarkStrategy.for_monotonous_timestamps(),
            source_name
        ).map(
            CreateRow(self._seperator, self._data_schema),
            output_type=Types.ROW_NAMED(list(self._attribute_names), [Types.STRING()] * len(self._data_schema))
        ).map(
            ParseData(self._data_schema), output_type=self._tuple_type
        )

    def _assign_id(self, data_stream: DataStream) -> DataStream:
        return data_stream.map(
            AssignId(self._id_attribute), output_type=self.tuple_type
        )

    def _assign_timestamp(self, data_stream: DataStream) -> DataStream:
        if self._ts_assigner:
            data_stream = data_stream.assign_timestamps_and_watermarks(
                WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(self._ts_assigner)
            )
        return data_stream

    def _get_stream_with_ground_truth(self, data_stream: DataStream) -> tuple[DataStream, DataStream]:
        output_tag = OutputTag("ground-truth-output", self.tuple_type)
        data_stream = data_stream.process(
            WriteToSideOutput(output_tag), self.tuple_type
        )
        ds_side_output = data_stream.get_side_output(output_tag)
        return data_stream, ds_side_output.map(
            lambda value: Row(**{key: val if val is None else str(val) for key, val in value.as_dict().items()}),
            Types.ROW_NAMED(list(self.attribute_names), [Types.STRING()] * (len(self._data_schema) + 1))
        )

    def get_csv_schema(self) -> CsvSchema:
        csv_schema = CsvSchema.builder()
        for attribute in self.attribute_names:
            csv_schema = csv_schema.add_string_column(attribute)

        return csv_schema.set_column_separator(self._seperator).build()
