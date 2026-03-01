import pandas as pd
from typing import Dict, List, Optional
from .validation_strategy import ValidationStrategy

class MandatoryValidation(ValidationStrategy):

    def __init__(
        self,
        config: Optional[Dict] = None
        ) -> None:
        super().__init__(config)

    def validate(self, df: pd.DataFrame, column: str, empty_list: Optional[List] = None) -> pd.Series:
        mask: pd.Series = df[column].isna()
        if empty_list:
            mask = mask | (df[column].isin(empty_list))
        return mask