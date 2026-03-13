import os
from pathlib import Path
import pandas as pd
from abc import ABC, abstractmethod
from loguru import logger
from typing import Dict, List, Union
from utils import logger_wrapper


class WriteDataStrategy(ABC):

    @abstractmethod
    @logger_wrapper
    def write_data(
        self,
        df: Union[[Dict, List[Dict]], pd.DataFrame],
        file_path: str,
        *args,
        **kwargs
        ) -> None:
        """Write data to file or database.

        Args:
            df (Union[[Dict, List[Dict]], pd.DataFrame]): data needs writing.
            file_name (str): file path locates.
        """
        pass

    @logger_wrapper
    def run(
        self,
        df: Union[[Dict, List[Dict]], pd.DataFrame],
        file_path: str,
        *args,
        **kwargs
    ) -> None:
        """Run function to write data.

        Args:
            df (Union[[Dict, List[Dict]], pd.DataFrame]): data needs writing.
            file_path (str): file path locates.
        """
        if df is None or df.empty:
            logger.warning(f"[{self.__class__.__name__}] data is empty")
            return
        if not os.path.exists(file_path):
            logger.info(f"[{self.__class__.__name__}] file/folder is not exists. Create new one: {file_path}.")
            output_file = Path(file_path)
            output_file.parent.mkdir(exist_ok=True, parents=True)

        self.write_data(df, file_path, *args, **kwargs)