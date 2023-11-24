import inspect
import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Builder.polluter_builder import LogOutputManager
from src.Logging.polluter_logger import OutputLogger
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.multiply_with_constant_attribute_polluter import \
    MultiplyWithConstant
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.replace_value_attribute_polluter import \
    ReplaceValue
from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner
from src.ReadWrite.CSV.reader_writer_csv import CsvReader
from src.Splitter.SplittedStream import SplittedStream
from src.Splitter.StreamCopySplitter import StreamCopySplitter

"""
Evaluation setup
"""

benchmark_name = "software_update"
benchmark_tag = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]

data_dir = "./data/evaluation"  # directory containing input data (schema file & seed data)
output_dir = "./data/evaluation_results"
output_dir = os.path.join(output_dir, benchmark_tag)

eval_data = os.path.join(data_dir, "eval_data_sample.tsv")
eval_schema = os.path.join(data_dir, "eval_schema.json")

"""
Flink environment
"""
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)

"""
Data source
"""

dataCsv = CsvReader(eval_data, eval_schema, '\t') \
    .set_ts_assigner(
    TabularTimestampAssigner("Time", TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S'))
)

"""
Split Streams
"""
ds = dataCsv.get_stream(env, benchmark_name)
print(ds.get_name())
stream_copy_splitter = StreamCopySplitter(3, ds)
splitted_stream = SplittedStream.split_stream(stream_copy_splitter, ds, 'myId')

"""
Perform Pollution on Streams
"""
logger = OutputLogger("myId", "myAttributeLog")  # Create Logger
logger_handler = LogOutputManager(logger)  # Create log handler

stream_tag_1 = splitted_stream.stream_tags[0]
stream_tag_2 = splitted_stream.stream_tags[1]
stream_tag_3 = splitted_stream.stream_tags[2]

mwc1 = MultiplyWithConstant(['CaloriesBurned'], [0], "myId").set_output_logger(logger)
mwc2 = MultiplyWithConstant(['CaloriesBurned'], [-1], "myId")
rv = ReplaceValue('CaloriesBurned', "myId", None).set_output_logger(logger)

splitted_stream.apply_polluter(stream_tag_1, mwc1)
splitted_stream.set_log_output_manager(stream_tag_1, logger_handler)

splitted_stream.apply_polluter(stream_tag_2, mwc2)
splitted_stream.apply_polluter(stream_tag_3, rv)
splitted_stream.set_log_output_manager(stream_tag_3, logger_handler)

ds = splitted_stream.merge_streams(True)
ds = ds.map(log_value('main'))  # optional console output of the polluted tuples. Only for debugging.

side = logger_handler.get_output_stream_by_logger(logger)  # get side output (log stream)
side = side.map(log_value('side'))

"""
Execute the Flink program
"""
env.execute()
