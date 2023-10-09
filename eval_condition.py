import inspect
from datetime import datetime
import os

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Builder.polluter_builder import PolluterBuilderComposite
from src.Conditions.probability_condition import ProbabilityCondition
from src.Polluters.TabularPolluters.StreamPolluters.AttributePolluter.replace_value_attribute_polluter import \
    ReplaceValue
from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner
from src.ReadWrite.CSV.reader_writer_csv import CsvReader, CsvWriter

benchmark_name = "conditions"
benchmark_tag = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]

data_dir = "./data/evaluation"
output_dir = "./data/evaluation_results"
output_dir = os.path.join(output_dir, benchmark_tag)

eval_data = os.path.join(data_dir, "eval_data.tsv")
eval_schema = os.path.join(data_dir, "eval_schema.json")


def create_polluter_name(attribute, polluter_type):
    return attribute + ": " + polluter_type


env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)

start = datetime.now()

# init data source
dataCsv = CsvReader(eval_data, eval_schema, '\t') \
    .set_ts_assigner(
    TabularTimestampAssigner("Time", TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S'))
)

ds = dataCsv.get_stream(env, benchmark_name + '_source')

err_probabilities = range(15, 75, 15)  # [15, 30, 45, 60] probabilities for conditions
conditions = [ProbabilityCondition(p / 100) for p in err_probabilities]  # create probability conditions

effected_attributes = ["BPM", "Steps", "CaloriesBurned", "ActiveMinutes"]  # polluted attributes

# create polluters
polluter_arguments = {"replacement": ""}
polluters = [
    ReplaceValue(attribute=attribute, id_attribute='myId', **polluter_arguments).set_condition(condition)
    .set_polluter_name(
        create_polluter_name(attribute, 'reduce precision'))
    for attribute, condition in
    zip(effected_attributes, conditions)
]

polluterBuilder = PolluterBuilderComposite() \
    .set_polluters(polluters)

ds = polluterBuilder.build(ds)

csv_writer = CsvWriter(output_dir, list(dataCsv.attribute_names))
ds = ds.map(log_value('main'))
csv_writer.write(ds)

env.execute()

print(datetime.now() - start)
