import pandas as pd
from typing import Optional, Union, Dict, List
from .base_strategy import ReadDataStrategy
from utils.logger import logger


class ReadExcelFileStrategy(ReadDataStrategy):
    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)

    # def load(
    #     self, sheet_name: Union[str, List[str]], *args, **kwargs
    # ) -> Optional[Union[Dict, pd.DataFrame]]:
    #     """Read excel data by chunk"""
    #     # In future, using polars instead pandas to load excel file
    #     chunk_size: int = kwargs.get("chunk_size", 10_000)
    #     # with pd.ExcelFile(self.file_path, engine=engine) as file:
    #     #     return pd.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

    #     # Read the header separately to maintain column consistency
    #     # df_header = pd.read_excel(
    #     #     self.file_path,
    #     #     sheet_name=sheet_name,
    #     #     nrows=1,
    #     #     engine="calamine",
    #     #     dtype=str
    #     # )
    #     columns = [i for i in pd.ExcelFile(self.file_path, engine="calamine").parse(sheet_name)]

    #     skiprows = 1 # Start skipping after the header row
    #     df = pd.DataFrame([], columns=columns)
    #     while True:
    #         # Read a chunk of data, skipping previous rows and without a header
    #         df_chunk = pd.read_excel(
    #             self.file_path,
    #             sheet_name=sheet_name,
    #             nrows=chunk_size,
    #             skiprows=skiprows,
    #             header=None,
    #             engine="calamine",
    #             *args,
    #             **kwargs
    #         )

    #         # If the chunk is empty, the end of the file is reached
    #         if not df_chunk.shape[0]:
    #             break

    #         # Assign the correct column names
    #         df_chunk.columns = columns

    #         # Process the chunk (e.g., append to a list, perform analysis)
    #         # yield df_chunk
    #         df = pd.concat([df, df_chunk], axis=0, ignore_index=True)

    #         skiprows += chunk_size

    #     return df

    def load(self, sheet_name: Union[str, List[str]], *args, **kwargs):
        chunk_size = kwargs.get("chunk_size", 10_000)
        usecols = kwargs.get("usecols")
        # Read the header separately to maintain column consistency
        with pd.ExcelFile(self.file_path, engine="calamine") as xf:
            columns = xf.parse(sheet_name, nrows=0).columns.tolist()
            if usecols is not None:
                invalid_columns = [col for col in usecols if col not in columns]
                if invalid_columns:
                    logger.error(
                        f"[{self.__class__.__name__}] Invalid columns: {invalid_columns}"
                    )
                    return

            skiprows = 1
            chunks = []
            while True:
                df_chunk = pd.read_excel(
                    xf,
                    sheet_name=sheet_name,
                    nrows=chunk_size,
                    skiprows=skiprows,
                    header=None,
                    engine="calamine",
                    dtype=str,
                )
                if df_chunk.empty:
                    break
                df_chunk.columns = columns
                if usecols is not None:
                    df_chunk = df_chunk[usecols]
                chunks.append(df_chunk)
                skiprows += chunk_size

            if not chunks:
                return pd.DataFrame(columns=columns)
            return pd.concat(chunks, ignore_index=True)
