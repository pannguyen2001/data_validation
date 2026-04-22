import pandas as pd
from loguru import logger
from typing import Optional
from utils.logger_wrapper import logger_wrapper
from .setup import validation_strategy_factory, outer_reference_registry


@logger_wrapper
def validate_data(df: pd.DataFrame = None, df_validation_config: pd.DataFrame = None, sheet_name: str = "") -> Optional[pd.DataFrame]:
    """
    Validate data and record result.
    """
    if df is None:
        logger.error(f"[{validate_data.__name__}] df is required.")
        return df
    if df_validation_config is None:
        logger.error(f"[{validate_data.__name__}] df_validation_config is required.")
        return df
    if not sheet_name:
        logger.error(f"[{validate_data.__name__}] sheet_name is required.")
        return df
    if df.empty:
        logger.warning(f"[{validate_data.__name__}] df is empty.")
        return df
    if df_validation_config.empty:
        logger.info(f"[{validate_data.__name__}] df_validation_config is empty. No validation action needs.")
        return df

    # for index, config in df_validation_config.iterrows():
    #     if config["type"] == "inner_reference":
    #         config["factory"] = validation_strategy_factory
    #     if config["type"] == "outer_reference":
    #         config["factory"] = validation_strategy_factory
    #         config["outer_reference_registry"] = outer_reference_registry
    #     validation_strategy_factory.get_strategy(config["type"])(df=df, **config).run()

    for index, row in df_validation_config.iterrows():
        config = row.to_dict()
        config["sheet_name"] = sheet_name
        if config["type"] == "inner_reference":
            config["factory"] = validation_strategy_factory
        if config["type"] == "outer_reference":
            config["factory"] = validation_strategy_factory
            config["outer_reference_registry"] = outer_reference_registry
        validation_strategy_factory.get_strategy(config["type"])(df=df, **config).run()
    return df
