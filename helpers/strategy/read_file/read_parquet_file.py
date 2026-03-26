import pandas as pd
from typing import Optional, Union, Dict
import pyarrow.parquet as pq
from .base_strategy import ReadDataStrategy

class ReadParquetFileStrategy(ReadDataStrategy):

    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)
    
    def load(self, *args, **kwargs) -> Optional[Union[Dict, pd.DataFrame]]:
        # return pd.read_parquet(self.file_path, *args, **kwargs)
        chunk_size: int = self.kwargs.get("chunk_size", 10_000)
        parquet_file = pq.ParquetFile(self.file_path, *args, **kwargs)
        df_chunks = []

        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            df_chunk = batch.to_pandas()
            df_chunks.append(df_chunk)

        df_chunks = pd.concat(df_chunks, ignore_index=True)
        return df_chunks