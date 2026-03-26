import pandas as pd
from abc import ABC, abstractmethod
from loguru import logger
from utils.logger_wrapper import logger_wrapper


class PreprocessingStrategy(ABC):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        self.df = df
        self.args = args
        self.kwargs = kwargs

        if self.df is None:
            raise ValueError(f"[{self.__class__.__name__}] df and config must be provided.")
        if self.df.empty:
            raise ValueError(f"[{self.__class__.__name__}] df must not be empty.")

    def is_empty(self, data) -> bool:
        """Check if data is empty"""
        if data is None or data.empty:
            return True
        return False

    @abstractmethod
    @logger_wrapper
    def process(self, column: str, *args, **kwargs) -> pd.DataFrame:
        """Processsing data"""
        raise NotImplementedError

    @logger_wrapper
    def run(self, *args, **kwargs) -> pd.DataFrame:
        column = self.kwargs.get("name")
        if not column:
            raise ValueError(f"[{self.__class__.__name__}] column is required.")
        if column not in self.df.columns:
            raise ValueError(
                f"[{self.__class__.__name__}] Column '{column}' not in df: {self.df.columns.values.tolist()}"
            )
        if self.is_empty(self.df[column]):
            logger.warning(f"[{column}] [{self.__class__.__name__}] Empty data. No processing.")
            return self.df

        self.process(column, *args, **kwargs)
        logger.success(f"[{column}] [{self.__class__.__name__}] Processing complete.")
        return self.df
