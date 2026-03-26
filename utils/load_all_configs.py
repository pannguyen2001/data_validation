import pandas as pd
from typing import Dict, Tuple
from .logger_wrapper import logger_wrapper
from .process_config import process_config
from pipeline.config_cache import ConfigCache
from configs.constants import PROCESSING_CONFIG_FOLDER_PATH, VALIDATION_CONFIG_FOLDER_PATH


@logger_wrapper
def load_all_configs(common_kwargs: Dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    validator_cache = ConfigCache()
    validator_cache.load_configs(PROCESSING_CONFIG_FOLDER_PATH, "processing")
    validator_cache.load_configs(VALIDATION_CONFIG_FOLDER_PATH, "validation")

    processing_cache = validator_cache.get(config_type="processing")
    validation_cache = validator_cache.get(config_type="validation")

    df_processing = pd.DataFrame(processing_cache)
    df_processing = process_config(df_processing, common_kwargs)

    df_validation = pd.DataFrame(validation_cache)
    df_validation = process_config(df_validation, common_kwargs)

    return df_processing, df_validation