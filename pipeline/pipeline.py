import pandas as pd
import numpy as np
import datetime
from loguru import logger
from typing import Dict, List, Any, Optional, Union, Literal
from .setup import read_file_strategy_factory
from utils import logger_wrapper, detec_file_type
from .setup import (
    preprocessing_strategy_factory,
    read_file_strategy_factory,
    validation_strategy_factory
)
from utils import logger, process_config, process_result

# ========== Data validation pipeline ==========
# Load config
# Load default data
# Setup global data: using for whole pipeline
# Setup scope data: using for single file -> single sheet
# Processing config data
# Load data
# Pre processing data
# Validation data
# Save report
# Post processing data 
# Save data


    


@logger_wrapper
def data_validation_pipeline(
    df: pd.DataFrame = None,
    df_processing: pd.DataFrame = None,
    df_validation_config: pd.DataFrame = None,
    df_constant_data: pd.DataFrame = None,
    *args,
    **kwargs
) -> None:
    """
    Pipeline for data validation, include these steps:
    1. Setup constant
    2. Load config
    3. Load default data
    """
    if df is None:
        raise ValueError(f"[{data_validation_pipeline.__name__}] df is required.")
    if df.empty:
        logger.warning(f"[{data_validation_pipeline.__name__}] df is empty.")
        return

    if df_processing is not None and not df_processing.empty:
        df = process_data(df, df_processing, processing_type_list)
        if df is None:
            logger.warning(f"[{data_validation_pipeline.__name__}] Processing data is failed")
            return

    # if df_validation_config is not None and not df_validation_config.empty:
    #     df = data_validation(df, df_validation_config)
    #     if df is None:
    #         logger.warning(f"[{data_validation_pipeline.__name__}] Validating data is failed")
    #         return


            


    

