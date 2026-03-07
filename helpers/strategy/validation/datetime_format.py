import pandas as pd
from .base_strategy import ValidationStrategy


class DatetimeFormatValidation(ValidationStrategy):
    def __init__(
        self, df: pd.DataFrame = None, *args, **kwargs
    ) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = "Check datetime format"
        self.kwargs["message"] = f"Incorrect datetime format. Correct format is {self.kwargs.get('format')}"

    def validate(self, column: str = "") -> pd.Series:
        datetime_format = self.kwargs.get("format")

        if not datetime_format:
            raise ValueError(f"[{self.__class__.__name__}] format is required.")

        mask: pd.Series = (
            self.df[column].notna()
            & pd.to_datetime(
                self.df[column],
                errors="coerce",
                format=datetime_format
            ).isna()
        )
        return mask
