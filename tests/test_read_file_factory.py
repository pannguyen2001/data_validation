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
from helpers.factory.read_file_factory import ReadFileStrategyFactory



class TestReadFileStrategyFactory(unittest.TestCase):

    def test_read_file_factory(self):
        read_file_strategy_factory = ReadFileStrategyFactory()

        excel_file_path: str = os.path.join(BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx")
        read_file_strategy_factory.register("excel", ReadExcelFileStrategy(excel_file_path))

        json_file_path: str = os.path.join(BASE_PATH, "configs", "learner_enum_data.json")
        read_file_strategy_factory.register("json", ReadJsonFileStrategy(json_file_path))

        yaml_file_path: str = os.path.join(BASE_PATH, "configs", "learner_account_validation_config.yaml")
        read_file_strategy_factory.register("yaml", ReadYAMLFileStrategy(yaml_file_path))

        assert read_file_strategy_factory.get_strategy("excel") is not None, "Excel strategy not found"
        assert read_file_strategy_factory.get_strategy("json") is not None, "JSON strategy not found"
        assert read_file_strategy_factory.get_strategy("yaml") is not None, "YAML strategy not found"

    def test_read_file_factory_failed(self):
        read_file_strategy_factory = ReadFileStrategyFactory()

        excel_file_path: str = os.path.join(BASE_PATH, "data", "test_data", "API learner profile template Dev.xlsx")
        read_file_strategy_factory.register("excel", ReadExcelFileStrategy(excel_file_path))

        assert read_file_strategy_factory.get_strategy("json") is not None, "JSON strategy not found"
        