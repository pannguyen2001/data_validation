import os
import unittest
import pandas as pd
from configs.constants import BASE_PATH
from helpers.strategy.read_file import (
    ReadDataStrategy,
    ReadExcelFileStrategy,
    ReadJsonFileStrategy,
    ReadYAMLFileStrategy
)

def read_file_wrapper(
    strategy_class: ReadDataStrategy,
    file_path: str = "",
    *args,
    **kwargs
    ):
        strategy = strategy_class(file_path)
        result = strategy.load(*args, **kwargs)
        assert isinstance(result, (dict, list, pd.DataFrame)), f"Incorrect result type. Expected: dict or list, Actual: {type(result)}."
        assert result is not None, "Result is empty."


class TestReadFileStrategy(unittest.TestCase):

    def test_read_yaml_file(self):
        file_path: str = os.path.join(BASE_PATH, "configs", "learner_account_validation_config.yaml")
        return read_file_wrapper(ReadYAMLFileStrategy, file_path)

    def test_read_json_file(self):
        file_path: str = os.path.join(BASE_PATH, "configs", "learner_enum_data.json")
        return read_file_wrapper(ReadJsonFileStrategy, file_path)

    def test_read_excel_file(self):
        file_path: str = os.path.join(BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx")
        sheet_name: str = "Edit learner profile result"
        return read_file_wrapper(ReadExcelFileStrategy, file_path, sheet_name)

    def test_lack_of_file_path(self):
        result = ReadExcelFileStrategy()
        assert result is None, "Result is empty."

    def test_file_path_is_empty(self):
        result = ReadExcelFileStrategy("")
        assert result is None, "Result is empty."

    def test_incorrect_file_path(self):
        incorrect_file_path: str = os.path.join(BASE_PATH, "data", "test_data", "API learner profile template.xlsx")
        result = ReadExcelFileStrategy(incorrect_file_path)
        assert result is None, "Result is empty."


    


    
