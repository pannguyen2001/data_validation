import pandas as pd
from .base_strategy import ValidationStrategy


class InRangeNumberValidation(ValidationStrategy):
    def __init__(
        self, df: pd.DataFrame = None, *args, **kwargs
    ) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check in range number"
        self.kwargs["message"] = self.kwargs.get("message") or f"Value should be between {self.kwargs.get('min_value')} and {self.kwargs.get('max_value')}"

    def validate(self, column: str = "") -> pd.Series:
        min_value = self.kwargs.get("min_value")
        max_value = self.kwargs.get("max_value")

        if min_value is None or max_value is None:
            raise ValueError(f"[{self.__class__.__name__}] min_value and max_value are required.")

        data = pd.to_numeric(self.df[column], errors="coerce")
        if max_value < min_value:
            min_value, max_value = max_value, min_value

        mask = data.notna() & ((data < min_value) | (data > max_value))
        return mask
