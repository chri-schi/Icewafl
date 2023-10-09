import pandas as pd
from statistics import mean
import os

subject_id = "0216-0051"
data_dir = "data_raw/S2_Data/Activity_Data"
mt = "MainTable_" + subject_id + "-NHC.tsv"
hrt = "HRTable_" + subject_id + "-NHC.tsv"
df_mt = pd.read_csv(os.path.join(data_dir, mt), sep='\t')
df_mt["Time"] = pd.to_datetime(df_mt["Time"])

df_hrt = pd.read_csv(os.path.join(data_dir, hrt), sep='\t')
df_hrt["Time"] = pd.to_datetime(df_hrt["Time"])

df_joined = df_hrt[['Time', 'BPM']].set_index("Time") \
    .resample("15min") \
    .agg([("BPM", mean)]) \
    .droplevel(0, axis=1) \
    .join(df_mt.set_index("Time"))

eval_data_dir = "data/evaluation"
schema_path = os.path.join(eval_data_dir, "eval_schema.json")
eval_data_path = os.path.join(eval_data_dir, "eval_data.tsv")

# Check whether the specified path exists or not
if not os.path.exists(eval_data_dir):
    os.makedirs(eval_data_dir)
    print("Evaluation data directory created!")

# Save Data
df_joined.to_csv(eval_data_path, sep='\t', header=False)

# Save Schema
with open(schema_path, 'w') as handle:
    handle.write(
        '{'
        + '"Time": "str",'
        + '"BPM": "float",'
        + '"ResearchID": "str",'
        + '"BiobankID": "str",'
        + '"Steps": "int",'
        + '"Distance": "float",'
        + '"Floors": "int",'
        + '"ActiveMinutes": "float",'
        + '"CaloriesBurned": "float",'
        + '"ActivityLevel": "str"'
        + '}'
    )