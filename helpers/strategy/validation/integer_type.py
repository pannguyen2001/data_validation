import pandas as pd
import numpy as np
from .base_strategy import ValidationStrategy


class IntergerTypeValidation(ValidationStrategy):
    INT_TYPE = (int, np.integer)

    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check integer type"
        self.kwargs["message"] = self.kwargs.get("message") or "Incorrect integer type"

    def validate(self, column: str = "") -> pd.Series:
        mask = self.df[column].notna() & (
            pd.to_numeric(self.df[column], errors="coerce").isna()
            | self.df[column].map(lambda x: any([i for i in str(x) if "." in i]))
        )
        return mask
