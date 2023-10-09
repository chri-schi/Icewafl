import calendar
from datetime import datetime
from typing import Any

from pyflink.common import TypeInformation, Types, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import MapFunction, RuntimeContext, ProcessFunction, OutputTag

from src.ReadWrite.reader_writer_base import PythonToFlinkTypeMapper


class CsvPythonToFlinkTypeMapper(PythonToFlinkTypeMapper):
    """
    Mapping between Python and Flink types.
    """
    def map(self, python_type) -> TypeInformation:
        match python_type:
            case "float":
                return Types.DOUBLE()
            case "int":
                return Types.LONG()
            case "bool":
                return Types.BOOLEAN()
            case _:
                return Types.STRING()


class AssignId(MapFunction):
    """
    Assigns an uid to each tuple.
    """

    def __init__(self, id_column: str):
        self._parallelism = -1
        self._id_counter = -1
        self.id_column = id_column

    def open(self, runtime_context: RuntimeContext):
        self._parallelism = runtime_context.get_number_of_parallel_subtasks()
        self._id_counter = runtime_context.get_index_of_this_subtask()

    def map(self, value: Row):
        self._id_counter += self._parallelism
        return Row(**{self.id_column: str(self._id_counter - self._parallelism)} | value.as_dict())


class ParseData(MapFunction):
    """
    Converts attributes to the data types specified in the schema provided by the user.
    """
    def __init__(self, schema):
        self.schema = schema

    def map(self, value):
        for attribute in self.schema:
            try:
                match self.schema[attribute]:
                    case "float":
                        value[attribute] = float(value[attribute])
                    case "int":
                        value[attribute] = int(value[attribute])
                    case "bool":
                        value[attribute] = bool(value[attribute])
                    case _:
                        value[attribute] = str(value[attribute])
            except ValueError:
                value[attribute] = None
        return value


class CreateRow(MapFunction):
    """
    Creates Rows from the input data. Rows are used as representation for tuples.
    """
    def __init__(self, separator, schema):
        self.separator = separator
        self.schema = schema

    def map(self, value: str) -> Row:
        init_tuple = {}
        attributes = value.split(self.separator)
        for idx, attribute in enumerate(self.schema):
            init_tuple[attribute] = '' if idx >= len(attributes) else attributes[idx]
        return Row(**init_tuple)


class WriteToSideOutput(ProcessFunction):
    """
    Writes data streams to a side output, e.g., ground truth or logs
    """
    def __init__(self, output_tag: OutputTag):
        self._output_tag = output_tag

    @property
    def get_output_tag(self):
        return self._output_tag

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        yield value
        yield self._output_tag, value


class TabularTimestampAssigner(TimestampAssigner):
    """
    Specify the time attribute of a tuple and parse its values. The parsed timestamps are then interpreted
    as event time.
    """
    # a transformation to milliseconds since the Java epoch of 1970-01-01T00:00:00Z may be required.
    def __init__(self, timestamp_column, transformation=lambda t: t):
        self._transformation = transformation
        self._timestamp_column = timestamp_column

    def extract_timestamp(self, value: Any, record_timestamp: int) -> int:
        return int(self._transformation(value[self._timestamp_column]) + 0.5)

    @staticmethod
    def transformation_timestamp_from_string(timestamp_format: str = '%Y-%m-%d %H:%M:%S'):
        def convert(timestamp: str):
            ts = datetime.strptime(timestamp, timestamp_format)
            return calendar.timegm(ts.utctimetuple()) * 1000

        return convert

    @staticmethod
    def transformation_convert_to_ms(scaling_factor: float = 1 / 1000000):
        def convert(timestamp: float):
            return timestamp * scaling_factor

        return convert
