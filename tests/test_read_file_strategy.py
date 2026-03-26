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

# =========== Test using unittest ==========
def read_file_wrapper(
    strategy_class: type[ReadDataStrategy],
    file_path: str = "",
    *args,
    **kwargs
    ):
        strategy = strategy_class(file_path)
        result = strategy.load(*args, **kwargs)
        if result is None:
            raise AssertionError("Result should not be None.")

        if not isinstance(result, (dict, list, pd.DataFrame)):
            raise AssertionError(
                f"Incorrect result type. Expected dict, list, or DataFrame. Actual: {type(result)}"
            )

        return result

class TestReadFileStrategy(unittest.TestCase):

    def test_read_yaml_file(self):
        file_path: str = os.path.join(BASE_PATH, "configs", "validation", "learner_account_validation_config.yaml")
        result = read_file_wrapper(ReadYAMLFileStrategy, file_path)
        self.assertIsInstance(result,  dict | list)
        self.assertTrue(len(result) > 0, "YAML result should not be empty.")

    def test_read_json_file(self):
        file_path: str = os.path.join(BASE_PATH, "configs", "constants", "learner_enum_data.json")
        result = read_file_wrapper(ReadJsonFileStrategy, file_path)

        self.assertIsInstance(result, (dict, list))
        self.assertTrue(len(result) > 0, "JSON result should not be empty.")

    def test_read_excel_file(self):
        file_path: str = os.path.join(BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx")
        sheet_name: str = "Edit learner profile result"
        result = read_file_wrapper(
            ReadExcelFileStrategy,
            file_path,
            sheet_name=sheet_name,
        )

        self.assertIsInstance(result, pd.DataFrame)
        self.assertFalse(result.empty, "Excel DataFrame should not be empty.")

    def test_lack_of_file_path(self):
        with self.assertRaises((TypeError, ValueError)):
            ReadExcelFileStrategy()

    def test_file_path_is_empty(self):
        # Choose contract: constructor validates OR load validates
        with self.assertRaises((ValueError, FileNotFoundError)):
            strategy = ReadExcelFileStrategy("")
            strategy.load()

    def test_incorrect_file_path(self):
        incorrect_file_path = os.path.join(
            BASE_PATH, "data", "test_data", "API learner profile template.xlsx"
        )

        with self.assertRaises(FileNotFoundError):
            strategy = ReadExcelFileStrategy(incorrect_file_path)
            strategy.load()


# ========== Test using Pytest ==========
import pytest

def test_read_yaml_file():
    file_path = os.path.join(
        BASE_PATH, "configs", "validation", "learner_account_validation_config.yaml"
    )

    strategy = ReadYAMLFileStrategy(file_path)
    result = strategy.load()

    assert isinstance(result, (dict, list))
    assert result

def test_read_json_file():
    file_path = os.path.join(
        BASE_PATH, "configs", "constants", "learner_enum_data.json"
    )

    strategy = ReadJsonFileStrategy(file_path)
    result = strategy.load()

    assert isinstance(result, (dict, list))
    assert result

def test_read_excel_file():
    file_path = os.path.join(
        BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx"
    )

    strategy = ReadExcelFileStrategy(file_path)
    result = strategy.load(sheet_name="Edit learner profile result")

    assert isinstance(result, pd.DataFrame)
    assert not result.empty


def test_excel_sheet_not_found():
    file_path = os.path.join(
        BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx"
    )

    strategy = ReadExcelFileStrategy(file_path)

    with pytest.raises((ValueError, KeyError)):
        strategy.load(sheet_name="NOT_EXISTS")


@pytest.mark.parametrize("bad_path", ["", None])
def test_excel_invalid_file_path(bad_path):
    with pytest.raises((ValueError, TypeError, FileNotFoundError)):
        strategy = ReadExcelFileStrategy(bad_path)
        strategy.load()