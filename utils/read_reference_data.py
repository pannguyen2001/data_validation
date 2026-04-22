import pandas as pd
from typing import Optional, List
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.detect_file_type import detect_file_type
from pipeline.setup import read_file_strategy_factory



@logger_wrapper
def read_reference_data(
    file_path: str = "",
    sheet_name: str = "",
    usecols: List = None,
    *args,
    **kwargs
) -> Optional[pd.DataFrame]:

    if not file_path:
        raise ValueError(
            f"[{read_reference_data.__name__}] file_path is required."
        )

    df = read_file_strategy_factory.get_strategy(
        detect_file_type(file_path)
    )(file_path).load(
        sheet_name=sheet_name,
        usecols=usecols,
        *args,
        **kwargs
    )

    if df is None:
        logger.error(
            f"[{read_reference_data.__name__}] Read data failed. Sheet: {sheet_name}, file: {file_path}"
        )
        return df

    if df.empty:
        logger.warning(
            f"[{read_reference_data.__name__}] Data is empty. Sheet: {sheet_name}, file: {file_path}"
        )
        return df

    logger.success(f"[{read_reference_data.__name__}] Read reference data successfully. Sheet: {sheet_name}, file: {file_path}")
    return df
