import pandas as pd
import numpy as np
from typing import Dict, List
from .base_strategy import ValidationStrategy


class ValueListValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check value list"
        self.kwargs["message"] = self.kwargs.get("message") or "Incorrect value"

    def validate(self, column: str = "") -> pd.Series:
        value_list = self.kwargs.get("values")
        if value_list is None:
            raise ValueError(f"[{self.__class__.__name__}] [{column}] value_list is required.")
        if not isinstance(value_list, (Dict, List, pd.DataFrame, pd.Series, np.array)):
            raise TypeError(
                f"[{self.__class__.__name__}] [{column}] value_list is incorrect type."
            )

        mask: pd.Series = ~self.df[column].isin(value_list) & self.df[column].notna()
        return mask
