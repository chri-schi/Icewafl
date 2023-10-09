import inspect
import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Conditions.function_condition import FunctionCondition
from src.Logging.polluter_logger import OutputLogger, LoggerConfig
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.composite_attribute_polluter import \
    CompositeAttributePolluter
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.multiply_with_constant_attribute_polluter import \
    MultiplyWithConstant
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.replace_value_attribute_polluter import \
    ReplaceValue
from src.ReadWrite.CSV.reader_writer_csv import CsvReader, CsvWriter
from src.Builder.polluter_builder import PolluterBuilderComposite, LogOutputManager

from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner

"""
Evaluation setup
"""
benchmark_name = "logging"
benchmark_tag = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]

data_dir = "./data/evaluation"  # directory containing input data (schema file & seed data)
output_dir = "./data/evaluation_results"
output_dir = os.path.join(output_dir, benchmark_tag)

eval_data = os.path.join(data_dir, "eval_data.tsv")
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

ds, ds_ground_truth = dataCsv.get_stream_with_ground_truth(env, 'csv-source')  # create stream with ground truth data

"""
Save ground truth data
"""
csv_writer_gt = CsvWriter(os.path.join(output_dir, 'gt'), list(dataCsv.attribute_names), "\t")
csv_writer_gt.write(ds_ground_truth)

"""
Create some attribute polluters with output logging
"""
logger = OutputLogger("myId", "myAttributeLog")  # Create Logger

test_log_polluter2 = ReplaceValue("Steps", "myId", None) \
    .set_condition(FunctionCondition(lambda val: val["myId"] == '0')) \
    .set_polluter_name("attributePol2") \
    .set_output_logger(logger)

test_log_composite_polluter = CompositeAttributePolluter(
    [
        test_log_polluter2,
        MultiplyWithConstant(["CaloriesBurned"], [-1.0], "myId").set_output_logger(logger),
        ReplaceValue("Floors", "myId", 666).set_output_logger(logger)
    ],
    "myId"
).set_condition(FunctionCondition(lambda v: v["myId"] == '0')).set_output_logger(logger)

"""
Pollution pipeline (Building)
"""
polluterBuilder = PolluterBuilderComposite() \
    .set_polluters([test_log_composite_polluter, test_log_polluter2])

logger_handler = LogOutputManager(logger)  # Create log handler

ds = polluterBuilder.build(ds, logger_handler=logger_handler)

ds = ds.map(log_value('main'))
side = logger_handler.get_output_stream_by_logger(logger)  # get side output (log stream)

"""
Save polluted data and logs 
"""
if side:
    side = side.map(log_value("side"))
    csv_writer_logs = CsvWriter(os.path.join(output_dir, 'log'), LoggerConfig.logged_attributes())
    csv_writer_logs.write(side)
else:
    print("not logged")

csv_writer = CsvWriter(output_dir, list(dataCsv.attribute_names))
csv_writer.write(ds)

env.execute()
