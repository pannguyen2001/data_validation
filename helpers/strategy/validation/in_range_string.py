import pandas as pd
from .base_strategy import ValidationStrategy


class InRangeStringLengthValidation(ValidationStrategy):
    def __init__(
        self, df: pd.DataFrame = None, *args, **kwargs
    ) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check string length"
        self.kwargs["message"] = self.kwargs.get("message") or f"String length should be between {int(self.kwargs.get('min_length'))} and {int(self.kwargs.get('max_length'))}"

    def validate(self, column: str = "") -> pd.Series:
        min_length = self.kwargs.get("min_length")
        max_length = self.kwargs.get("max_length")

        if min_length is None or max_length is None:
            raise ValueError(f"[{self.__class__.__name__}] min_length and max_length are required.")

        if max_length < min_length:
            min_length, max_length = max_length, min_length

        mask = self.df[column].notna() & (
            (self.df[column].str.len() < min_length)
            | (self.df[column].str.len() > max_length)
        )
        return mask
