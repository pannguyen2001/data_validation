import pandas as pd
from .base_strategy import ValidationStrategy


class DatetimeLogicValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or f"Check datetime logic between {self.kwargs.get('left_column')} and {self.kwargs.get('right_column')}"
        self.kwargs["message"] = self.kwargs.get("message") or f"{self.kwargs.get('left_column')} is later than {self.kwargs.get('right_column')}"

    def validate(self, column: str = "") -> pd.Series:
        left_column: str = self.kwargs.get("left_column")
        right_column: str = self.kwargs.get("right_column")
        datetime_format = self.kwargs.get("format")

        if left_column is None or right_column is None or datetime_format is None:
            raise ValueError(f"[{self.__class__.__name__}] left_column, right_column and datetime format are required. {left_column = }, {right_column = }, {datetime_format = }")
        if left_column not in self.df.columns or right_column not in self.df.columns:
            raise ValueError(f"[{self.__class__.__name__}] left_column or right_column notr in data column. {left_column = }, {right_column = }, {self.df.columns = }")

        mask = pd.to_datetime(
            self.df[left_column], errors="coerce", format=datetime_format
        ) > pd.to_datetime(
            self.df[right_column], errors="coerce", format=datetime_format
        )
        return mask


# @logger_wrapper
# def validate_effective_time_logic(
#     df: pd.DataFrame,
#     eff_from_col: str,
#     eff_to_col: str,
#     datetime_format: str
# ) -> pd.DataFrame:
#     mask = pd.to_datetime(df[eff_from_col], errors="coerce", format=datetime_format) > pd.to_datetime(df[eff_to_col], errors="coerce", format=datetime_format)
#     if any(mask):
#         mark_result(
#             df=df,
#             mask=mask,
#             column=f"{eff_from_col} - {eff_to_col}",
#             validation_type="Check time logic",
#             message=f"{eff_from_col} must be before {eff_to_col}"
#         )
#     return df
