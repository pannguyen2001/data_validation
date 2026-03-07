import pandas as pd
from .base_strategy import PreprocessingStrategy
from utils import logger_wrapper


class RemoveWhiteSpaceProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        """
        Remove white space in column
        """
        mask = self.df[column].notna()
        self.df.loc[mask, column] = self.df.loc[mask, column].astype(str).str.strip()
        return self.df
