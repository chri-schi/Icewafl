# Eiswafl

The purpose of Eiswafl ( **E**rror **I**njection **S**olution **w**ith **A**pache **Fl**ink) is to create data quality (DQ) benchmarks from streaming or
batch data. Thereby, Eiswafl pollutes provided seed data with different error patterns. This pollution process is highly
configurable and allows to generate benchmark data with different error types and error rates. In addition, it is
possible to adjust the behaviour of the pollution process depending on attribute values in the data, event time or
even a random variable. With its highly configurable pollution process, Eiswafl is designed to
generate benchmarks for a wide range of real-world scenarios.

## Prerequisites (Windows 10)

1. Install Python 3.10.11
2. Install Java 11
3. Create an environment variable JAVA_HOME that points to your Java 11 installation.
4. Install PyFlink (https://pypi.org/project/apache-flink/) (pip install apache-flink==1.17.0).

## Execute Examples

1. Create a folder named `data_raw` in the same dictionary as this readme file lies.
2. Download the fitness tracker data (https://doi.org/10.1371/journal.pbio.2004285.s005).
3. Unpack the content of the downloaded zip file into the `data_raw` folder.
4. Install pandas (https://pypi.org/project/pandas/) (pip install pandas==1.3.5).
5. Execute the `prepare_evaluation.py` script. The script creates the folder `data` containing the files required for the
   examples.
6. Run one of the five provided examples:
    1. `eval_condition.py`
    2. `eval_delay_tuple.py`
    3. `eval_software_update.py` (This example is a good starting point because it best explains the basic structure of pollution pipelines.)
    4. `eval_temporal.py`
    5. `eval_logging.py`
7. The result of the example is in a folder named after the name of the example. This folder is located in the folder
   `evaluation_results`, which in turn is located in the folder `data` created by the script `prepare_evaluation.py`.