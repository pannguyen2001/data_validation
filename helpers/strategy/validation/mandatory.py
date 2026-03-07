import pandas as pd
from .base_strategy import ValidationStrategy


class MandatoryValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check mandatory"
        self.kwargs["message"] = self.kwargs.get("message") or "Required field"

    def validate(self, column: str) -> pd.Series:
        empty_list = self.kwargs.get("empty_list")
        mask: pd.Series = self.df[column].isna()

        if empty_list:
            mask = mask | (self.df[column].isin(empty_list))

        return mask
