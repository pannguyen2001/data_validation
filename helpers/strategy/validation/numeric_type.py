import pandas as pd
from .base_strategy import ValidationStrategy


class NumericTypeValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = "Check numeric type"
        self.kwargs["message"] = "Incorrect type"

    def validate(self, column: str = "") -> pd.Series:
        mask = (
            self.df[column].notna()
            & pd.to_numeric(self.df[column], errors="coerce").isna()
        )
        return mask
