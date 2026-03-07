import json
import pandas as pd
from loguru import logger
from typing import List
from utils import logger_wrapper
from .setup import preprocessing_strategy_factory

PROCESSING_TYPE: List = [
    "remove_white_space",
    "string_case",
    "split_string",
    "fill_default",
    "enum_mapping",
]


@logger_wrapper
def process_data(
    df: pd.DataFrame = None,
    df_processing_config: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Processing dataframe.
    - Remove white space
    - String case
    - Split string
    - Fill default value
    - Enum mapping
    """
    if df is None or df_processing_config is None:
        raise ValueError(
            f"[{process_data.__name__}] df or df_processing_config is required."
        )
        return
    if df.empty or df_processing_config.empty:
        logger.error(f"[{process_data.__name__}] df or df_processing_config is empty.")
        return df

    for item in PROCESSING_TYPE:
        processing_columns = df_processing_config.loc[
            df_processing_config["type"] == item
        ]
        if processing_columns.empty:
            logger.warning(
                f"[{process_data.__name__}] No columns to take action {item}."
            )
            continue

        logger.info(
            f"[{process_data.__name__}] [{item}] Process {processing_columns['name'].shape[0]}/{df.columns.shape[0]} columns: {json.dumps(processing_columns['name'].tolist(), indent=4)}"
        )
        processing_strategy = preprocessing_strategy_factory.get_strategy(item)
        for index, config in processing_columns.iterrows():
            df = processing_strategy(df, **config).run()
    return df
