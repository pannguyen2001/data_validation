import pandas as pd
from typing import Optional, Union, Dict
from .base_strategy import ReadDataStrategy

class ReadParquetFileStrategy(ReadDataStrategy):

    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)
    
    def load(self, *args, **kwargs) -> Optional[Union[Dict, pd.DataFrame]]:
        return pd.read_parquet(self.file_path, *args, **kwargs)