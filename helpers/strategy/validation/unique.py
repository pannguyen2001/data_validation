import pandas as pd
import numpy as np
from typing import List, Union
from .base_strategy import ValidationStrategy


class UniqueValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check unique"
        self.kwargs["message"] = self.kwargs.get("message") or "Duplicate value"

    def validate(self, column: Union[List[str], str]) -> pd.Series:
        if isinstance(column, str):
            mask = self.df[column].duplicated(keep=False) & self.df[column].notna()
            return mask

        data = self.df[column].copy()
        for column_name in column:
            if data[column_name].isna().all():
                data = data.drop(columns=[column_name])
        mask = data.duplicated(keep=False)

        return mask
