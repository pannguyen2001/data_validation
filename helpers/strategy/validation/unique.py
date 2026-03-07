import pandas as pd
from .base_strategy import ValidationStrategy


class UniqueValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Unique"
        self.kwargs["message"] = self.kwargs.get("message") or "Duplicate value"

    def validate(self, column: str = "") -> pd.Series:
        mask = self.df[column].duplicated(keep="first") & self.df[column].notna()
        return mask
