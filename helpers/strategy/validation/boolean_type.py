import pandas as pd
import numpy as np
from typing import Dict
from .base_strategy import ValidationStrategy


BOOLEAN_MAPPING: Dict = {
    "true": True,
    "false": False
}

class BooleanTypeValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check numeric type"
        self.kwargs["message"] = self.kwargs.get("message") or "Incorrect boolean type"

    def validate(self, column: str = "") -> pd.Series:
        mask = (
            self.df[column].notna()
            & self.df[column].astype(str).str.strip().str.lower().map(lambda x: BOOLEAN_MAPPING.get(x, np.nan))
        )
        return mask
