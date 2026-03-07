import pandas as pd
from utils import logger_wrapper
from typing import Dict
from .base_strategy import PreprocessingStrategy


class EnumMappingProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        enum_values = self.kwargs.get("enum_values")
        if enum_values is None:
            raise ValueError(f"[{self.__class__.__name__}] enum_values is required.")
        if not isinstance(enum_values, Dict):
            raise TypeError(f"[{self.__class__.__name__}] enum_values must be dict.")

        self.df[column] = self.df[column].astype("object")

        mask = self.df[column].notna()
        self.df.loc[mask, column] = self.df.loc[mask, column].map(enum_values)
        # self.df[column] = self.df[column].convert_dtypes()
        return self.df
