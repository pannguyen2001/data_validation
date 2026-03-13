import pandas as pd
from .base_strategy import PreprocessingStrategy
from utils import logger_wrapper
from typing import List


CASE_TYPE: List = [
    "lower",
    "upper",
    "title",
    "capitalize"
]

class StringCaseProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        case_type = self.kwargs.get("case_type")
        if not case_type:
            raise ValueError(f"[{self.__class__.__name__}] case_type is required.")
        if case_type not in CASE_TYPE:
            raise TypeError(f"[{self.__class__.__name__}] case_type must be in {CASE_TYPE}.")

        mask = self.df[column].notna()

        match case_type:
            case "upper":
                self.df.loc[mask, column] = self.df.loc[mask, column].str.upper()
            case "lower":
                self.df.loc[mask, column] = self.df.loc[mask, column].str.lower()
            case "title":
                self.df.loc[mask, column] = self.df.loc[mask, column].str.title()
            case "capitalize":
                self.df.loc[mask, column] = self.df.loc[mask, column].str.capitalize()
            case _:
                raise ValueError(
                    f" [{self.__class__.__name__}] Invalid case type: {case_type}"
                )
        return self.df
