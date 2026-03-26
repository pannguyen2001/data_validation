import pandas as pd
from .base_strategy import PreprocessingStrategy
from utils.logger_wrapper import logger_wrapper
from typing import List, Dict


DATA_TYPE: List = [
    "integer",
    "numeric",
    "boolean",
    "string"
]

BOOLEAN_MAPPING: Dict = {
    True: "true",
    False: "false"
}

# For post processing, because when read data, all are string, so to process next phase, some column must convert to correct dtype: int, float, bool, ...
class ConvertDataTypeProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        data_type = self.kwargs.get("data_type")
        decimal_places = self.kwargs.get("decimal_places")
        if not data_type:
            raise ValueError(f"[{self.__class__.__name__}] data_type is required.")
        if data_type not in DATA_TYPE:
            raise TypeError(f"[{self.__class__.__name__}] data_type '{data_type}' is incorrect. Value must be in {DATA_TYPE}.")
        if data_type == "numeric" and decimal_places is None:
            raise ValueError(f"[{self.__class__.__name__}] decimal_places is required if data type id numeric.")
        if pd.isna(pd.to_numeric(decimal_places, errors="coerce")):
            raise TypeError(f"[{self.__class__.__name__}] decimal_places must be integer.")

        mask = self.df[column].notna()

        match data_type:
            case "integer":
                self.df[column] = pd.to_numeric(self.df[column], errors='coerce') # Convert column to object or numeric first to allow assignment
                self.df.loc[mask, column] = self.df.loc[mask, column].astype(int)
            case "numeric":
                self.df[column] = self.df[column].astype(object) # Force the whole column to float/object first so it can accept float assignments
                self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
                self.df.loc[mask, column] = (
                    self.df.loc[mask, column]
                    .astype(float)
                    .round(int(decimal_places))
                )
            case "boolean":
                self.df[column] = self.df[column].astype(object) # Ensure the mapping handles the conversion without dtype conflicts
                self.df.loc[mask, column] = self.df.loc[mask, column].map(lambda x: BOOLEAN_MAPPING.get(x, x))
            case "string":
                self.df.loc[mask, column] = self.df.loc[mask, column].astype(str).str.strip()
            case _:
                raise ValueError(
                    f" [{self.__class__.__name__}] Invalid case type: {data_type}"
                )
        return self.df
