import pandas as pd
from utils.logger_wrapper import logger_wrapper
from .base_strategy import PreprocessingStrategy


class FillDefaultValueProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        default_value = self.kwargs.get("default_value")

        if not default_value:
            raise ValueError(f"[{self.__class__.__name__}] default_value is required.")

        self.df[column] = self.df[column].fillna(default_value)
        return self.df
