import pandas as pd
import numpy as np
from typing import Dict, List
from .base_strategy import ValidationStrategy
from .integer_type import IntergerTypeValidation
from .numeric_type import NumericTypeValidation
from .boolean_type import BooleanTypeValidation


DATA_TYPE_CHECKING_LIST: List = ["integer", "numeric", "boolean"]

class DataTypeValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check data type"
        self.kwargs["message"] = self.kwargs.get("message") or "Incorrect data type: "

    def validate(self, column: str = "") -> pd.Series:
        data_type = self.kwargs.get("data_type")
        self.kwargs["message"] = self.kwargs["message"] + data_type
        if data_type is None:
            raise ValueError(f"[{self.__class__.__name__}] [{column}] data_type is required.")
        if data_type not in DATA_TYPE_CHECKING_LIST:
            raise ValueError(f"[{self.__class__.__name__}] [{column}] data_type '{data_type}' is incorrect. Value must be one of these: {DATA_TYPE_CHECKING_LIST}.")
        
        match data_type:
            case "integer":
                mask = IntergerTypeValidation(self.df, self.args, self.kwargs).validate(column)
            case "numeric":
                mask = NumericTypeValidation(self.df, self.args, self.kwargs).validate(column)
            case "boolean":
                mask = BooleanTypeValidation(self.df, self.args, self.kwargs).validate(column)
        return mask
