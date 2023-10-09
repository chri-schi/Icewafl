import inspect
from datetime import datetime
import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Builder.polluter_builder import PolluterBuilderComposite
from src.Conditions.function_condition import FunctionCondition
from src.Conditions.probability_condition import ProbabilityCondition
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.composite_attribute_polluter import \
    CompositeAttributePolluter
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.multiply_with_constant_attribute_polluter import \
    MultiplyWithConstant
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.reduce_precision_attribute_polluter import \
    ReducePrecision
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.replace_value_attribute_polluter import \
    ReplaceValue
from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner
from src.ReadWrite.CSV.reader_writer_csv import CsvReader, CsvWriter

"""
Evaluation setup
"""
benchmark_name = "software update"
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

# start time of the evaluation
start = datetime.now()

"""
Data source
"""
dataCsv = CsvReader(eval_data, eval_schema, '\t') \
    .set_ts_assigner(
    TabularTimestampAssigner("Time", TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S'))
)

ds = dataCsv.get_stream(env, benchmark_name + '_source')


def software_update_condition_fct(value) -> bool:
    """
    This function is later used inside a function condition. The function demonstrates that conditions based on time
    are not necessarily dependent on Flink's event time. Here, the time attribute is handled just like an
    'ordinary' attribute.
    """
    ts = TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S')('2016-02-27 00:00:00')
    ts_val = TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S')(value['Time'])
    return ts_val >= ts


"""
Pollution pipeline (definition)
"""

software_update_condition = FunctionCondition(software_update_condition_fct)  # condition

convert_distance_to_mm = MultiplyWithConstant(["Distance"], [1000 * 100], "myId")  # polluter
reduce_calories_burned = ReducePrecision("CaloriesBurned", 2, "myId")  # polluter

# Composite polluter harbouring two polluters. The composite polluter has a condition that controls the two registered
# polluters. For one of the registered polluters an additional condition is specified. This condition is only evaluated
# if the condition of the composite polluter evaluates to true.
sub_polluter = CompositeAttributePolluter(
    [ReplaceValue('BPM', 'myId', 0), ReplaceValue('BPM', 'myId', "").set_condition(ProbabilityCondition(0.20))],
    "myId"
).set_condition(
    FunctionCondition(lambda val: val["BPM"] > 100)
)

# Another composite polluter containing the above composite polluter and two other polluters. The registered condition
# defines the day the software update took place.
software_update_polluter = CompositeAttributePolluter(
    [convert_distance_to_mm, reduce_calories_burned, sub_polluter],
    "myId"
).set_condition(software_update_condition).set_polluter_name("software update")

"""
Pollution pipeline (Building)
"""
polluterBuilder = PolluterBuilderComposite() \
    .set_polluters([software_update_polluter])

# Converts the pollution pipeline to transformation operators used in the streaming data flow of Flink
ds = polluterBuilder.build(ds)

"""
Data sink 
"""
csv_writer = CsvWriter(output_dir, list(dataCsv.attribute_names))
ds = ds.map(log_value('main'))  # optional console output of the polluted tuples. Only for debugging.
csv_writer.write(ds)

"""
Execute the Flink program
"""
env.execute()

"""
Duration of the pollution process.
"""
print(datetime.now() - start)
