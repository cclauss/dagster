import os

from pyspark.sql import Row, DataFrame as SparkDF

from dagster_pyspark import spark_session_from_config

from dagster.utils.temp_file import get_temp_dir

from lakehouse import PySparkMemLakehouse, lakehouse_table, lakehouse_table_input_def

from .common import LocalOnDiskSparkCsvLakehouse, execute_spark_lakehouse_build

# Note typehints in lakehouse purely optional and behave as vanilla typehints


@lakehouse_table
def TableOne(context) -> SparkDF:
    return context.resources.spark.createDataFrame([Row(num=1)])


@lakehouse_table
def TableTwo(context) -> SparkDF:
    return context.resources.spark.createDataFrame([Row(num=2)])


@lakehouse_table(
    input_defs=[
        lakehouse_table_input_def('table_one', TableOne),
        lakehouse_table_input_def('table_two', TableTwo),
    ]
)
def TableThree(_, table_one: SparkDF, table_two: SparkDF) -> SparkDF:
    return table_one.union(table_two)


def test_execute_in_mem_lakehouse():
    lakehouse = PySparkMemLakehouse()
    pipeline_result = execute_spark_lakehouse_build(
        tables=[TableOne, TableTwo, TableThree], lakehouse=lakehouse
    )

    assert pipeline_result.success

    assert lakehouse.collected_tables == {
        'TableOne': [Row(num=1)],
        'TableTwo': [Row(num=2)],
        'TableThree': [Row(num=1), Row(num=2)],
    }


def test_execute_file_system_lakehouse():
    with get_temp_dir() as temp_dir:
        pipeline_result = execute_spark_lakehouse_build(
            tables=[TableOne, TableTwo, TableThree],
            lakehouse=LocalOnDiskSparkCsvLakehouse(temp_dir),
        )

        assert pipeline_result.success

        def get_table(name):
            spark = spark_session_from_config()
            return spark.read.csv(
                os.path.join(temp_dir, name), header=True, inferSchema=True
            ).collect()

        assert get_table('TableOne') == [Row(num=1)]
        assert get_table('TableTwo') == [Row(num=2)]
        assert set(get_table('TableThree')) == set([Row(num=1), Row(num=2)])
