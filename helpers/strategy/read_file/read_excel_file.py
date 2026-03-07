import pandas as pd
from typing import Optional, Union, Dict, List
from .base_strategy import ReadDataStrategy


class ReadExcelFileStrategy(ReadDataStrategy):
    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)

    def load(
        self, sheet_name: Union[str, List[str]], *args, **kwargs
    ) -> Optional[Union[Dict, pd.DataFrame]]:
        engine = kwargs.get("engine", "openpyxl")
        with pd.ExcelFile(self.file_path, engine=engine) as file:
            return pd.read_excel(file, sheet_name=sheet_name, *args, **kwargs)
