import os
import openpyxl
import pandas as pd
import polars as pl
from loguru import logger
from pathlib import Path
from utils.logger_wrapper import logger_wrapper
from .base_strategy import WriteDataStrategy

class WriteToCSVStrategy(WriteDataStrategy):

    @logger_wrapper
    def write_data(
        self,
        df: pd.DataFrame,
        file_path: str,
        sheet_name: str = "Sheet1",
        *args,
        **kwargs
        ) -> None:
            logger.info(f"[{self.__class__.__name__}] Write data to csv file.")
            folder = Path(file_path).parent
            des_file_path = str(folder / f"{sheet_name}.csv")
            pl.from_pandas(df).write_csv(des_file_path, include_header=True, separator=",")

            logger.success(f"Write data to csv file successfully: {des_file_path}.")
