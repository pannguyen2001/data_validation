import pandas as pd
from .base_strategy import ValidationStrategy


class ValueListValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = "Check value list"
        self.kwargs["message"] = "Incorrect value"

    def validate(self, column: str = "") -> pd.Series:
        value_list = self.kwargs.get("values")
        if not value_list:
            raise ValueError(f"[{self.__class__.__name__}] [{column}] value_list is required.")
        if not isinstance(value_list, object):
            raise TypeError(
                f"[{self.__class__.__name__}] [{column}] value_list is incorrect type."
            )

        mask: pd.Series = ~self.df[column].isin(value_list) & self.df[column].notna()
        return mask
