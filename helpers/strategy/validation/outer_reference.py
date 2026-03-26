import pandas as pd
from typing import Dict, Optional, Union
from helpers.strategy.validation.base_strategy import ValidationStrategy
from helpers.factory.validation import ValidationStrategyFactory
from helpers.factory.read_file_factory import ReadFileStrategyFactory
from utils.logger_wrapper import logger_wrapper
from utils.condition_parser import ConditionParser
from utils.detect_file_type import detect_file_type
from utils.mark_result import mark_result


class OuterReferenceRegistry:
    def __init__(self, read_file_strategy_factory: ReadFileStrategyFactory = None) -> None:
        self.read_file_strategy_factory = read_file_strategy_factory
        self.cache = {}
        if not self.read_file_strategy_factory:
            raise ValueError(f"[{self.__class__.__name__}] read_file_strategy_factory is required.")

    @logger_wrapper
    def get_data(
        self,
        file_path: str,
        *args,
        **kwargs
        ) -> Union[pd.DataFrame, pd.Series]:
        file_type: str = detect_file_type(file_path)
        sheet_name: str = kwargs.get("sheet_name")
        if file_type == "excel" and not sheet_name:
            raise ValueError(f"[{self.__class__.__name__}] sheet_name is required for excel file.")

        key = (file_path, sheet_name)

        if key not in self.cache:
            read_file_strategy = self.read_file_strategy_factory.get_strategy(file_type)
            df = read_file_strategy(file_path=file_path).load(*args, **kwargs)
            self.cache[key] = df

        return self.cache[key]

class OuterReferenceValidation(ValidationStrategy):
    def __init__(
      self,
      df: pd.DataFrame = None,
      factory: Optional[ValidationStrategyFactory] = None,
      outer_reference_registry: Optional[OuterReferenceRegistry] = None,
      *args,
      **kwargs
      ) -> None:
      super().__init__(df, args, kwargs)
      self.df = df
      self.args = args
      self.kwargs = kwargs
      self.ref_info = self.kwargs.get("ref_info")
      self.factory = factory
      self.outer_reference_registry = outer_reference_registry
      self.compiled_refs = []
      self.kwargs["validation_type"] = "Check outer reference"
      self.marks_own_results = True

      if not self.ref_info:
        raise ValueError("ref_info is required")

      for ref in self.ref_info:
          condition = ref.get("condition")
          sub_rules = [
                self.factory.build_strategy(
                    rule.get("type"),
                    df=self.df,
                    **rule
                    )
                for rule in ref.get("ref_rules", [])
            ]

          self.compiled_refs.append({
                "condition": condition,
                "rules": sub_rules
            }) # recursion

    @logger_wrapper
    def reference_mask(self, df: pd.DataFrame, current_column: str, config: Dict) -> Union[pd.Series, pd.Series]:
        operator = config["operator"].upper()
        conditions = config["conditions"]

        masks = []

        for condition in conditions:
            file_path: str = condition.get("file_path")
            sheet_name: str = condition.get("sheet_name")
            ref_column = condition.get("column")
            filters = condition.get("conditions")

            df_ref = self.outer_reference_registry.get_data(file_path=file_path, sheet_name=sheet_name)

            if filters:
                for f in filters:
                    mask_ref = ConditionParser.build_mask(df_ref, f)
                    df_ref = df_ref.loc[mask_ref]

            if ref_column not in df_ref.columns:
                raise ValueError(f"Column {ref_column} not found in reference dataframe")

            lookup_set = df_ref[ref_column].dropna().drop_duplicates()

            mask = df[current_column].isin(lookup_set)

            masks.append(mask)

        if not masks:
          return pd.Series(False, index=df.index)

        result = masks[0]
        for mask in masks[1:]:
          result = ConditionParser.LOGICAL_MAP[operator](result, mask)

        return result, lookup_set

    @logger_wrapper
    def validate(
      self,
      column: str
      ) -> pd.Series:
        final_mask = pd.Series(False, index=self.df.index)

        for ref in self.compiled_refs:

            condition_mask, value_list = self.reference_mask(self.df, column, ref.get("condition"))
            # print(condition_mask) # trturn true if valid else false, temporatory, 2026-03-09, just outer reference that suspension reason in internal suspended list is in suspension reson > suspension reason name or not, so just get value list is enough
            # Apply nested rules only when condition true
            for rule in ref.get("rules"):
                if rule.__class__.__name__ in ["ValueListValidation"]:
                    rule.kwargs["values"] = value_list

                sub_mask = rule.validate(column)

                # invalid_mask = condition_mask & sub_mask

                # final_mask = final_mask | invalid_mask

                final_mask = final_mask | sub_mask

                mark_result(
                    self.df,
                    final_mask,
                    column,
                    self.kwargs["validation_type"],
                    rule.kwargs["message"]
                )

        return final_mask


    # @logger_wrapper
    # def load_outer_reference_data(
    #     self,
    #     file_path: str,
    #     *args,
    #     **kwargs
    #     ) -> pd.DataFrame:
    #     file_type = detect_file_type(file_path)
    #     file_loader_strategy_factory = ReadFileStrategyFactory()
    #     file_loader_strategy_factory.register("excel", ReadExcelFileStrategy)
    #     file_loader_strategy_factory.register("csv", ReadCSVFileStrategy)
    #     file_loader_strategy_factory.register("parquet", ReadParquetFileStrategy)

    #     file_loader_strategy = file_loader_strategy_factory.get_strategy(file_type, file_path=file_path)(*args, **kwargs).load()
        
    #     return file_loader_strategy