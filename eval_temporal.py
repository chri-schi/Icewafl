import inspect
from datetime import datetime
import os
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Builder.polluter_builder import PolluterBuilderComposite
from src.Conditions.cosine_temporal_condition import CosineCondition
from src.Conditions.probability_condition import ProbabilityCondition
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.replace_value_attribute_polluter import \
    ReplaceValue
from src.Polluters.TabularPolluters.StreamPolluters.time_dependent_polluter import TimeDependentStreamPolluter
from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner
from src.ReadWrite.CSV.reader_writer_csv import CsvReader, CsvWriter

"""
Configurate evaluation setup
"""
benchmark_name = "time dependent pollution"
benchmark_tag = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]

data_dir = "./data/evaluation"
output_dir = "./data/evaluation_results"
output_dir = os.path.join(output_dir, benchmark_tag)

eval_data = os.path.join(data_dir, "eval_data.tsv")
eval_schema = os.path.join(data_dir, "eval_schema.json")

"""
Begin evaluation
"""
env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)
start = datetime.now()

# initialise data source with timestamp assignment
dataCsv = CsvReader(eval_data, eval_schema, '\t') \
    .set_ts_assigner(
    TabularTimestampAssigner(
        "Time",
        TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S')
    )
)

ds = dataCsv.get_stream(env, benchmark_name + 'source')

arguments = {"replacement": ""}
polluter = ReplaceValue("Distance", "myId", **arguments).set_condition(ProbabilityCondition(0.5))

# A time dependent stream polluter that wraps an attribute polluter is created.
time_dependent_polluter = TimeDependentStreamPolluter(
    polluter,
    CosineCondition(lambda ts: datetime.fromtimestamp(ts / 1000).hour, 24)  # Condition based on event time
)

polluterBuilder = PolluterBuilderComposite() \
    .set_polluters([time_dependent_polluter])

ds = polluterBuilder.build(ds)

csv_writer = CsvWriter(output_dir, list(dataCsv.attribute_names))
ds = ds.map(log_value('main'))
csv_writer.write(ds)

env.execute()

print(datetime.now() - start)
