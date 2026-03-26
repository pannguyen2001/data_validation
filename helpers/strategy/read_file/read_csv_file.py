import pandas as pd
from typing import Optional, Union, Dict, List
from .base_strategy import ReadDataStrategy

class ReadCSVFileStrategy(ReadDataStrategy):

    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)
    
    def load(self, *args, **kwargs) -> Optional[Union[Dict, pd.DataFrame]]:
        chunk_size: int = self.kwargs.get("chunk_size") or 100_000
        chunks: List = []
        for chunk in pd.read_csv(self.file_path, chunksize=chunk_size, *args, **kwargs):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        return df

