import os
import pandas as pd
from loguru import logger
from utils import logger_wrapper
from string import Template
from .base_strategy import WriteDataStrategy


write_successfully_template = Template("""Write data to excel file successfully.
Detail info:
    Sheet name: ${sheet_name}.
    File: ${file_path}.
    Mode: ${mode_flag}.
    If sheet exists: ${if_sheet_exists}.""")

class WriteToExcelStrategy(WriteDataStrategy):
    # def __init__(self, file_path: str, sheet_name: str):
    #     super().__init__(file_path, sheet_name)

    @logger_wrapper
    def write_data(
        self,
        df: pd.DataFrame,
        file_path: str,
        sheet_name: str = "Sheet1",
        index: bool = False,
        mode: str = "replace",
        *args,
        **kwargs
        ) -> None:
            logger.info(f"[{self.__class__.__name__}] Write data to excel file.")
            mode_flag = "w" if not os.path.exists(file_path) else "a"
            if_sheet_exists = "replace" if mode == "replace" else "overlay"
            config = {
                "engine": kwargs.get("engine") or "openpyxl",
                "mode": mode_flag,
                **kwargs
            }
            if mode_flag == "a":
                config["if_sheet_exists"] = if_sheet_exists
            with pd.ExcelWriter(file_path, **config) as writer:
                df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=index,
                    *args,
                    **kwargs
                    )
            logger.success(write_successfully_template.safe_substitute(
                sheet_name=sheet_name,
                file_path=file_path,
                mode_flag=mode_flag,
                if_sheet_exists=if_sheet_exists
            ))