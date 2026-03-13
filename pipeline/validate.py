import pandas as pd
from loguru import logger
from typing import Optional
from utils import logger_wrapper
from .setup import validation_strategy_factory, outer_reference_registry


@logger_wrapper
def validate_data(df: pd.DataFrame = None, df_validation_config: pd.DataFrame = None) -> Optional[pd.DataFrame]:
    """
    Validate data and record result.
    """
    if df is None or df_validation_config is None:
        logger.error(f"[{validate_data.__name__}] df and df_validation_config is required.")
        return df
    if df.empty or df_validation_config.empty:
        logger.error(f"[{validate_data.__name__}] df or df_validation_config is empty.")
        return df

    for index, config in df_validation_config.iterrows():
        if config["type"] == "inner_reference":
            config["factory"] = validation_strategy_factory
        if config["type"] == "outer_reference":
            config["factory"] = validation_strategy_factory
            config["outer_reference_registry"] = outer_reference_registry
        validation_strategy_factory.get_strategy(config["type"])(df=df, **config).run()

    return df
