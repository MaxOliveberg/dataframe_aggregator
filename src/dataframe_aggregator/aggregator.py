import dataclasses
from dataclasses import dataclass
import pandas as pd
import numpy as np


@dataclass
class WriteSettings:
    running_writes: bool = False
    separate_files: bool = False
    separate_files_start_index: int = 0
    filename: str = "aggregated_output"
    meta_data_filename: str = filename + "_metadata"
    path: str = ""


@dataclass
class MetaData:
    num_frames: int


class MissingFieldException(Exception):
    def __init__(self, field_name):
        self.field_name = field_name


class InconsistentShapesException(Exception):
    def __init__(self, expected, actual):
        self.expected_shape = expected
        self.actual_shape = actual


class FramesUnorderedException(Exception):
    pass


class Aggregator:
    def __init__(self, identifier_fields, write_settings=None):
        self.__write_settings = write_settings
        if self.__write_settings is None:
            self.__write_settings = WriteSettings()
        self.__id = identifier_fields
        self.__write_index = self.__write_settings.separate_files_start_index

    def aggregate(self, aggregated_file: pd.DataFrame, additional_data: pd.DataFrame, meta_data: MetaData = None):
        if self.__check_init(aggregated_file, additional_data):
            return self.__initialise_frames(aggregated_file, additional_data)
        self.__check_fields(aggregated_file, additional_data, self.__id)
        self.__check_format(aggregated_file, additional_data)
        n = meta_data.num_frames
        new_frame = pd.DataFrame()
        new_frame[self.__id] = additional_data[self.__id]
        interesting_fields = [x for x in list(additional_data.columns) if x not in self.__id]
        for field in interesting_fields:
            new_frame[field] = (aggregated_file[field] * n + additional_data[field]) / (n + 1)
            # Welford's online algorithm for unbiased sample variande
            new_frame[field + "_var"] = aggregated_file[field + "_var"] + (1 / n) * (
                    additional_data[field] - aggregated_file[field]) ** 2 - (1 / (n - 1)) * (
                                            aggregated_file[field + "_var"])
        if self.__write_settings.running_writes:
            index = str(self.__write_index) if self.__write_settings.separate_files else ""
            path_and_name = self.__write_settings.path + self.__write_settings.filename + index
            new_frame.to_csv(path_and_name + ".csv")
            pd.DataFrame([dataclasses.asdict(meta_data)]).to_csv(
                path_and_name + self.__write_settings.meta_data_filename + ".csv")
            self.__write_index += 1
        return new_frame, MetaData(num_frames=n + 1)

    @staticmethod
    def __check_fields(aggregated_file: pd.DataFrame, additional_data: pd.DataFrame, id):
        for field_name in additional_data:
            if field_name not in aggregated_file:
                raise MissingFieldException(field_name=field_name)
            elif field_name + "_var" not in aggregated_file and field_name not in id:
                raise MissingFieldException(field_name=field_name + "_var")

    @staticmethod
    def __check_format(aggregated_file, additional_data):
        if aggregated_file.shape[0] != additional_data.shape[0]:
            raise InconsistentShapesException(expected=aggregated_file.shape, actual=additional_data.shape)

    @staticmethod
    def __check_init(aggregated_file: pd.DataFrame, additional_data: pd.DataFrame):
        for field in list(aggregated_file.columns):
            if field not in list(additional_data):
                return False
        return True

    def __initialise_frames(self, aggregated_file, additional_data):
        new_frame = pd.DataFrame()
        new_frame[self.__id] = additional_data[self.__id]
        interesting_fields = [x for x in list(additional_data.columns) if x not in self.__id]
        print(interesting_fields)
        for field in interesting_fields:
            new_frame[field] = (aggregated_file[field] + additional_data[field]) / 2
            new_frame[field + "_var"] = np.sqrt(
                (aggregated_file[field] - new_frame[field]) ** 2 + (additional_data[field] - new_frame[field]) ** 2)
        return new_frame, MetaData(num_frames=2)
