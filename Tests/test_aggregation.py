import pandas as pd
import numpy as np
import pytest

from src.dataframe_aggregator.aggregator import Aggregator


def generate_toy_data(amount, expected_values=None, standard_deviations=None):
    if expected_values is None:
        expected_values = np.array([0, 0])
    if standard_deviations is None:
        standard_deviations = np.array([1, 1])
    data_points = np.array([np.random.normal(expected_values, standard_deviations) for _ in range(amount)])
    return pd.DataFrame(data_points)


def test_stochastic_variable_aggregation():
    #  This is a flaky test, but it's very, very unlikely to fail if things are working correctly
    ev_epsilon = 0.05
    var_epsilon = 0.05
    num_iterations = 2 * 10 ** 4
    aggregator = Aggregator(identifier_fields=[])
    frame, meta = aggregator.aggregate(generate_toy_data(10), generate_toy_data(10))
    for i in range(num_iterations):
        frame, meta = aggregator.aggregate(frame, generate_toy_data(10), meta)
    for index, row in frame.iterrows():
        assert ev_epsilon > abs(row[0]) and ev_epsilon > abs(row[1])
        assert var_epsilon > abs(row["0_var"] - 1) and ev_epsilon > abs(row["1_var"] - 1)
