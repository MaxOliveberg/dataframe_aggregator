import dataclasses
from dataclasses import dataclass
import pandas as pd
import numpy as np


@dataclass
class WriteSettings:
    """
    Dataclass to hold settings that define how the aggregator should write to file.

    Fields:
        running_writes: bool
            If the Aggregator should write to file after each .aggregate(args).
        separate_files: bool
            If the Aggregator should write each aggregation to a separate file.
        separate_files_start_index: int
            If separate_files = True, files are separated by an index. This attribute defines the start index.
        filename: str
            The output file name.
        meta_data_filename: str
            The metadata output file name.
        path: str
            The path where to write the file and metadata file.
    """
    running_writes: bool = False
    separate_files: bool = False
    separate_files_start_index: int = 0
    filename: str = "aggregated_output"
    meta_data_filename: str = filename + "_metadata"
    path: str = ""


@dataclass
class MetaData:
    """
    Contains additional information about the aggregated data frames

    Fields:
        num_frames: int
            How many data frames have been aggregated into the returned frame

    """
    num_frames: int


class MissingFieldException(Exception):
    """
    Thrown in case a field is missing

    Fields:
        field_name: str
            The name of the missing field
    """

    def __init__(self, field_name):
        self.field_name = field_name


class InconsistentShapesException(Exception):
    """
    Thrown in case of inconsistent data frame lengths.

    Fields:
        expected: int
            The expected length
        actual: int
            The actual length
    """

    def __init__(self, expected, actual):
        self.expected_shape = expected
        self.actual_shape = actual


class FramesUnorderedException(Exception):
    """
    Todo: Is currently NOT thrown.
    Thrown in case of unordered frames.
    """
    pass


class Aggregator:
    """
    Class used to aggregate data frames

    Methods:
        aggregate(self, aggregated_file: Pandas.DataFrame,
                additional_data: Pandas.DataFrame, meta_data: MetaData = None): Pandas.DataFrame, MetaData

                Combines the two given frame into one, where each field is the mean of the two and additional fields
                denoting the sample variance are added.

                If aggregated_file already contains the variance fields, the additional_data will be added to the
                average. In this case, a MetaData must be given to denote the number of frames already averaged.
    """

    def __init__(self, identifier_fields, write_settings=None):
        """
        The parameter identifier_fields is a list of keys that define which fields should NOT be averaged.
        :param identifier_fields: [str]
        :param write_settings: WriteSettings
        """
        self.__write_settings = write_settings
        if self.__write_settings is None:
            self.__write_settings = WriteSettings()
        self.__id = identifier_fields
        self.__write_index = self.__write_settings.separate_files_start_index

    def aggregate(self, aggregated_file: pd.DataFrame, additional_data: pd.DataFrame, meta_data: MetaData = None):
        """
        Combines the two given frame into one, where each field is the mean of the two and additional fields
        denoting the sample variance are added.

        If aggregated_file already contains the variance fields, the additional_data will be added to the
        average. In this case, a MetaData must be given to denote the number of frames already averaged.

        :param aggregated_file: Pandas.DataFrame
        :param additional_data: Pandas.DataFrame
        :param meta_data: MetaData
        :return: Pandas.DataFrame, MetaData
        """
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
            # Welford's online algorithm for unbiased sample variance
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
    def __check_fields(aggregated_file: pd.DataFrame, additional_data: pd.DataFrame, identifier_fields):
        """
        Makes sure that the correct fields are present. Otherwise, it will raise a MissingFieldException.
        :param aggregated_file: Pandas.DataFrame
        :param additional_data: Pandas.DataFrame
        :param identifier_fields: [str]
        :raises: MissingFieldException
        :return: None
        """
        for field_name in additional_data:
            if field_name not in aggregated_file:
                raise MissingFieldException(field_name=field_name)
            elif field_name + "_var" not in aggregated_file and field_name not in identifier_fields:
                raise MissingFieldException(field_name=field_name + "_var")

    @staticmethod
    def __check_format(aggregated_file, additional_data):
        """
        Makes sure that the two DataFrames have the correct dimensions. Otherwise, a InconsistentShapesException is
        raised.
        :param aggregated_file: Pandas.DataFrame
        :param additional_data: Pandas.DataFrame
        :return: None
        """
        if aggregated_file.shape[0] != additional_data.shape[0]:
            raise InconsistentShapesException(expected=aggregated_file.shape, actual=additional_data.shape)

    @staticmethod
    def __check_init(aggregated_file: pd.DataFrame, additional_data: pd.DataFrame):
        """
        Checks to see if the given aggregated_dataframe is not in fact aggregated. If so, we need to run slightly
        different logic
        :param aggregated_file: Pandas.DataFrame
        :param additional_data: Pandas.DataFrame
        :return: bool
        """
        for field in list(aggregated_file.columns):
            if field not in list(additional_data):
                return False
        return True

    def __initialise_frames(self, aggregated_file, additional_data):
        """
        If the aggregated_file is not aggregated, we need to run slightly different logic for the first iteration.
        :param aggregated_file: Pandas.DataFrame
        :param additional_data: Pandas.DataFrame
        :return: Pandas.DataFrame, MetaData
        """
        new_frame = pd.DataFrame()
        new_frame[self.__id] = additional_data[self.__id]
        interesting_fields = [x for x in list(additional_data.columns) if x not in self.__id]
        print(interesting_fields)
        for field in interesting_fields:
            new_frame[field] = (aggregated_file[field] + additional_data[field]) / 2
            new_frame[field + "_var"] = np.sqrt(
                (aggregated_file[field] - new_frame[field]) ** 2 + (additional_data[field] - new_frame[field]) ** 2)
        return new_frame, MetaData(num_frames=2)
