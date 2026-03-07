import pandas as pd
from typing import Dict, Optional
from helpers.strategy.validation.base_strategy import ValidationStrategy
from helpers.factory.validation import ValidationStrategyFactory
from helpers.factory.read_file_factory import ReadFileStrategyFactory
from helpers.strategy.read_file import (
    ReadExcelFileStrategy,
    ReadCSVFileStrategy,
    ReadParquetFileStrategy
)
from utils.logger_wrapper import logger_wrapper
from utils.condition_parser import ConditionParser


class OuterReferenceRegistry:
    def __init__(self, read_file_strategy: ReadFileStrategyFactory) -> None:
        self.read_file_strategy = read_file_strategy
        self.cache = {}

    def get_column_values(
        self,
        file_name: str,
        column: str,
        *args,
        **kwargs
        ) -> pd.Series:

        key = (file_name, column)

        if key not in self.cache:
            df = self.file_loader(file_name=file_name, *args, **kwargs)
            self.cache[key] = df[column].dropna().drop_duplicates()

        return self.cache[key]

class OuterReferenceValidation(ValidationStrategy):
    def __init__(
      self,
      df: pd.DataFrame = None,
      factory: Optional[ValidationStrategyFactory] = None,
      # outer_reference_registry: Optional[OuterReferenceRegistry] = None
      *args,
      **kwargs
      ) -> None:
      super().__init__(df, args, kwargs)
      self.df = df
      self.args = args
      self.kwargs = kwargs
      self.ref_info = self.kwargs.get("ref_info")
      self.factory = factory
      # self.outer_reference_registry = outer_reference_registry
      self.compiled_refs = []

      if not self.ref_info:
        raise ValueError("ref_info is required")

      for ref in self.ref_info:
          condition = ref.get("condition")
          sub_rules = [
                self.factory.build_strategy(rule.get("type"), rule)
                for rule in ref.get("ref_rules", [])
            ]

          self.compiled_refs.append({
                "condition": condition,
                "rules": sub_rules
            }) # recursion

    @logger_wrapper
    def load_outer_reference_data(
        self,
        file_path: str,
        *args,
        **kwargs
        ) -> pd.DataFrame:
        file_type = file_path.split(".")[-1]
        file_loader_strategy_factory = ReadFileStrategyFactory()
        file_loader_strategy_factory.register("excel", ReadExcelFileStrategy)
        file_loader_strategy_factory.register("csv", ReadCSVFileStrategy)
        file_loader_strategy_factory.register("parquet", ReadParquetFileStrategy)

        if file_type == "csv":
            file_loader_strategy = file_loader_strategy_factory.get_strategy("csv", file_path=file_path)
            return file_loader_strategy(*args, **kwargs).load()
        if file_type == "parquet":
            file_loader_strategy = file_loader_strategy_factory.get_strategy("parquet", file_path=file_path)
            return file_loader_strategy(*args, **kwargs).load()
        if file_type == "xlsx" or file_type == "xls":
            file_loader_strategy = file_loader_strategy_factory.get_strategy("excel", file_path=file_path)
            return file_loader_strategy(*args, **kwargs).load()
        else:
            file_loader_strategy_factory.register(file_type, file_path=file_path)
            file_loader_strategy = file_loader_strategy_factory.get_strategy(file_type, file_path=file_path)
            return file_loader_strategy(*args, **kwargs).load()

    @logger_wrapper
    def reference_mask(self, df: pd.DataFrame, current_column: str, config: Dict) -> pd.Series:
        operator = config["operator"].upper()
        conditions = config["conditions"]

        masks = []

        for condition in conditions:
            file_path: str = condition["file_path"]
            sheet_name: str = condition["sheet_name"]
            ref_column = condition["column"]
            filters = condition["conditions"]

            df_ref = self.load_outer_reference_data(file_path=file_path, sheet_name=sheet_name)

            if filters:
                for f in filters:
                    mask_ref = ConditionParser.build_mask(df_ref, f)
                    df_ref = df_ref.loc[mask_ref]

            if ref_column not in df.columns:
                raise ValueError(f"Column {ref_column} not found in reference dataframe")

            lookup_set = df_ref[ref_column].dropna().drop_duplicates()

            mask = df[current_column].isin(lookup_set)

            masks.append(mask)

        if not masks:
          return pd.Series(False, index=df.index)

        result = masks[0]
        for mask in masks[1:]:
          result = ConditionParser.LOGICAL_MAP[operator](result, mask)

        return result

    @logger_wrapper
    def validate(
      self,
      df: pd.DataFrame,
      column: str
      ) -> pd.Series:
        final_mask = pd.Series(False, index=df.index)

        for ref in self.compiled_refs:

            condition_mask = self.reference_mask(df, column, ref["condition"])

            # Apply nested rules only when condition true
            for rule in ref["rules"]:

                sub_mask = rule.validate(df)

                invalid_mask = condition_mask & sub_mask

                final_mask = final_mask | invalid_mask

        return final_mask
