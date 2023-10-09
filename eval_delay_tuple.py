import inspect
from datetime import datetime
import os
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

from evaluation_helper import log_value
from src.Builder.polluter_builder import PolluterBuilderComposite
from src.Conditions.polluter_temporal_condition import TimeDependentPolluterCondition
from src.Conditions.probability_condition import ProbabilityCondition
from src.Polluters.TabularPolluters.KeyedStreamPolluters.delay_tuple_batch_temporal_polluter import DelayTupleBatch
from src.ReadWrite.CSV.read_write_utils import TabularTimestampAssigner
from src.ReadWrite.CSV.reader_writer_csv import CsvReader, CsvWriter

"""
Configurate evaluation setup
"""
benchmark_name = "time dependent pollution"
benchmark_tag = os.path.splitext(os.path.basename(inspect.getfile(inspect.currentframe())))[0]
print(__file__)

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


class CustomCondition(TimeDependentPolluterCondition):
    def condition(self, timestamp_ms: float) -> bool:
        hour = datetime.fromtimestamp(timestamp_ms / 1000).hour
        return 13 <= hour <= 14


# init data source
dataCsv = CsvReader(eval_data, eval_schema, '\t') \
    .set_ts_assigner(
    TabularTimestampAssigner("Time",
                             TabularTimestampAssigner.transformation_timestamp_from_string('%Y-%m-%d %H:%M:%S'))
)

ds = dataCsv.get_stream(env, benchmark_name + 'source')

polluter_delay = DelayTupleBatch(dataCsv.tuple_type, "myId", 1 * 60 * 60 * 1000) \
    .set_time_dependent_condition(CustomCondition()) \
    .set_condition(ProbabilityCondition(0.2))

polluterBuilder = PolluterBuilderComposite() \
    .set_polluters([polluter_delay])

ds = polluterBuilder.build(ds)

csv_writer = CsvWriter(output_dir, list(dataCsv.attribute_names))
ds = ds.map(log_value('main'))
csv_writer.write(ds)

env.execute()

print(datetime.now() - start)
