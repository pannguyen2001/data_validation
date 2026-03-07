import pandas as pd
from .base_strategy import ValidationStrategy


class InRangeDateTimeValidation(ValidationStrategy):
    def __init__(
        self, df: pd.DataFrame = None, *args, **kwargs
    ) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = "Check datetime range"
        self.kwargs["message"] = f"Datetime should be between {self.kwargs.get('start_date')} and {self.kwargs.get('end_date')}"

    def validate(self, column: str = "") -> pd.Series:
        start_date = self.kwargs.get("start_date")
        end_date = self.kwargs.get("end_date")
        datetime_format = self.kwargs.get("format")

        if not start_date or not end_date or not datetime_format:
            raise ValueError(
                f"[{self.__class__.__name__}] start_date, end_date, and format are required."
            )

        start_date = pd.to_datetime(
            start_date,
            errors="coerce",
            format=datetime_format
        )
        end_date = pd.to_datetime(
            end_date,
            errors="coerce",
            format=datetime_format
        )

        if end_date < start_date:
            start_date, end_date = end_date, start_date

        data = pd.to_datetime(
            self.df[column],
            errors="coerce",
            format=datetime_format
        )

        mask = data.notna() & ((data < start_date) | (data > end_date))
        return mask
