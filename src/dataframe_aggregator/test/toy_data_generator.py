from dataclasses import dataclass
import numpy as np


@dataclass
class ExampleDataclass:
    identifier: str
    data: np.ndarray


def generate_toy_data(amount, expected_values=None, standard_deviations=None):
    if expected_values is None:
        expected_values = np.array([0, 0])
    if standard_deviations is None:
        standard_deviations = np.array([1, 1])
    ret = []
    for i in range(amount):
        ret.append(ExampleDataclass(identifier=str(i), data=np.random.normal(expected_values, standard_deviations)))
    return ret


if __name__ == "__main__":
    some_data = generate_toy_data(100, expected_values=np.array([0, 1]), standard_deviations=np.array([1, 1]))
    print(some_data)
