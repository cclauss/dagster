import pandas as pd

import dagster
import dagster_ge

from dagster.core.decorators import solid
from dagster.core.execution import (execute_pipeline_in_memory, DagsterExecutionFailureReason)

from dagster.utils.test import script_relative_path

import dagster.pandas_kernel as dagster_pd


def _sum_solid_impl(num_df):
    sum_df = num_df
    sum_df['sum'] = sum_df['num1'] + sum_df['num2']
    return sum_df


def col_exists(name, col_name):
    return dagster_ge.ge_expectation(name, lambda ge_df: ge_df.expect_column_to_exist(col_name))


@solid(
    inputs=[
        dagster_pd.dataframe_input(
            'num_df',
            expectations=[col_exists('num1_exists', 'num1')],
        )
    ],
    output=dagster_pd.dataframe_output()
)
def sum_solid(num_df):
    return _sum_solid_impl(num_df)


@solid(
    inputs=[
        dagster_pd.dataframe_input(
            'num_df',
            expectations=[col_exists('failing', 'not_a_column')],
        ),
    ],
    output=dagster_pd.dataframe_output()
)
def sum_solid_fails_expectation(num_df):
    return _sum_solid_impl(num_df)


@solid(
    inputs=[
        dagster_pd.dataframe_input(
            'num_df',
            expectations=[
                dagster_ge.json_config_expectation(
                    'num_expectations', script_relative_path('num_expectations.json')
                )
            ],
        )
    ],
    output=dagster_pd.dataframe_output()
)
def sum_solid_expectations_config(num_df):
    return _sum_solid_impl(num_df)


def test_single_node_passing_expectation():
    in_df = pd.DataFrame.from_dict({'num1': [1, 3], 'num2': [2, 4]})
    pipeline = dagster.pipeline(solids=[sum_solid])

    result = execute_pipeline_in_memory(dagster.context(), pipeline, input_values={'num_df': in_df})
    assert result.success
    assert result.result_list[0].success
    assert result.result_list[0].transformed_value.to_dict('list') == {
        'num1': [1, 3],
        'num2': [2, 4],
        'sum': [3, 7],
    }


def test_single_node_passing_json_config_expectations():
    in_df = pd.DataFrame.from_dict({'num1': [1, 3], 'num2': [2, 4]})
    pipeline = dagster.pipeline(solids=[sum_solid_expectations_config])

    result = execute_pipeline_in_memory(dagster.context(), pipeline, input_values={'num_df': in_df})
    assert result.success
    assert result.result_list[0].success
    assert result.result_list[0].transformed_value.to_dict('list') == {
        'num1': [1, 3],
        'num2': [2, 4],
        'sum': [3, 7],
    }


def test_single_node_failing_expectation():
    in_df = pd.DataFrame.from_dict({'num1': [1, 3], 'num2': [2, 4]})
    pipeline = dagster.pipeline(solids=[sum_solid_fails_expectation])
    result = execute_pipeline_in_memory(
        dagster.context(), pipeline, input_values={'num_df': in_df}, throw_on_error=False
    )
    assert not result.success
    assert len(result.result_list) == 1
    first_solid_result = result.result_list[0]
    assert not first_solid_result.success
    assert first_solid_result.reason == DagsterExecutionFailureReason.EXPECTATION_FAILURE
    assert len(first_solid_result.failed_expectation_results) == 1
    expt_result = first_solid_result.failed_expectation_results[0]
    assert expt_result.result_context == {'success': False}