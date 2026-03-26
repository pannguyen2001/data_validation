import pandas as pd
from utils.logger_wrapper import logger_wrapper
from .base_strategy import PreprocessingStrategy


class SplitStringProcessing(PreprocessingStrategy):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        super().__init__(df, *args, **kwargs)

    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        separator = self.kwargs.get("separator")
        if not separator:
            raise ValueError(f"[{self.__class__.__name__}] separator is required.")

        if pd.api.types.is_string_dtype(self.df[column]):
            # Convert to object to allow list storage
            self.df[column] = self.df[column].astype(object)

        mask = self.df[column].notna()
        self.df.loc[mask, column] = (
            self.df.loc[mask, column]
            .astype(str)
            .str.split(separator)
            .map(lambda x: [i.strip() for i in x])
        )
        return self.df
