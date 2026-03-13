import json
import pandas as pd
from loguru import logger
from typing import List
from utils import logger_wrapper
from .setup import processing_strategy_factory

PROCESS_ACTION: List = ["preprocessing", "postprocessing"]

PROCESSING_TYPE: List = [
    "remove_white_space",
    "string_case",
    "split_string",
    "fill_default",
    "enum_mapping",
    "convert_data_type"
]


@logger_wrapper
def process_data(
    df: pd.DataFrame = None,
    df_processing_config: pd.DataFrame = None,
    processing_action: str = "preprocessing",
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
            f"[{processing_action}] df or df_processing_config is required."
        )
        return
    if df.empty or df_processing_config.empty:
        logger.error(f"[{processing_action}] df or df_processing_config is empty.")
        return df

    if processing_action not in PROCESS_ACTION:
        logger.error(
            f"[{processing_action}] incorrect processing type: {processing_action}. Value must be in list: {PROCESS_ACTION}."
        )
        return df

    for item in PROCESSING_TYPE:
        processing_columns = df_processing_config.loc[
            (
                (df_processing_config["type"] == item)
                & (df_processing_config["processing_action"] == processing_action)
            )
        ]
        if processing_columns.empty:
            logger.warning(
                f"[{processing_action}] No columns to take action {item}."
            )
            continue

        logger.info(
            f"[{processing_action}] [{item}] Process {processing_columns['name'].shape[0]}/{df.columns.shape[0]} columns: {json.dumps(processing_columns['name'].tolist(), indent=4)}"
        )
        processing_strategy = processing_strategy_factory.get_strategy(item)
        for index, config in processing_columns.iterrows():
            df = processing_strategy(df, **config).run()
    return df
