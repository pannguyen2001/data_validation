import pandas as pd
import numpy as np
from typing import Dict, List, Any, Set
from .base_strategy import ValidationStrategy


def check_row(val: Any, allowed_values: Set) -> bool:
    # 1. Check if it's a list first. 
    # A list is NEVER 'isna', so we can safely process it.
    if isinstance(val, list):
        # Return True (Error) if any element in the list is NOT allowed
        return not set(val).issubset(allowed_values)
    
    # 2. If it's NOT a list, then check if it's NaN/Null
    if pd.isna(val):
        return False # Assuming NaNs are handled by a 'Required' rule
    
    # 3. Finally, check single values (strings, ints, etc.)
    return val not in allowed_values

class ValueListValidation(ValidationStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)
        self.kwargs["validation_type"] = self.kwargs.get("validation_type") or "Check value list"
        self.kwargs["message"] = self.kwargs.get("message") or "Incorrect value"

    def validate(self, column: str = "") -> pd.Series:
        value_list = self.kwargs.get("values")
        separator = self.kwargs.get("separator")
        data = self.df[column]

        if value_list is None:
            raise ValueError(f"[{self.__class__.__name__}] [{column}] value_list is required.")
        
        if separator is not None and pd.notna(separator):
            data = (
            self.df[column]
            .astype(str)
            .str.split(separator)
            .map(lambda x: [i.strip() for i in x])
        )
        
        # Ensure value_list is a set for O(1) lookup performance
        allowed_values = set(value_list)

        # Apply the check row by row
        # mask = True means there is a validation ERROR
        mask = data.map(lambda x: check_row(x, allowed_values))

        return mask