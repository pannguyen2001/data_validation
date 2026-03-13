import pandas as pd
from loguru import logger
from utils import logger_wrapper, process_result, detect_file_type
from configs.constants import date_today, time_today, report_folder_path
from .processing import process_data
from .validate import validate_data
from .setup import write_data_strategy_factory

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
    df_processing_config: pd.DataFrame = None,
    df_validation_config: pd.DataFrame = None,
    file_path: str = "",
    sheet_name: str = "",
    *args,
    **kwargs
) -> None:
    """
    Pipeline for data validation, include these steps:
    1. Preprocess data.
    2. Validate data.
    3. Write report if having invalid data.
    4. Post processing data.

    Args:
        df (pd.DataFrame): input data.
        df_processing_config (pd.DataFrame): processing config info.
        df_validation_config (pd.DataFrame): validation config info.
        file_path (str): data file path.
        sheet_name (str): sheet name of data, if file is excel file.

    Return:
        df(pd.DataFrame): valid data after processing and validation.
    """
    if not file_path:
        raise ValueError(f"[{data_validation_pipeline.__name__}] file_path is required.")
    if df is None:
        raise ValueError(f"[{data_validation_pipeline.__name__}] df is required.")
    if df.empty:
        logger.warning(f"[{data_validation_pipeline.__name__}] df is empty.")
        return

    file_name: str = file_path.split("/")[-1].split(".")[0] or f"{date_today}_{time_today}"
    file_type: str = detect_file_type(file_path)

    if not file_type:
        raise NotImplementedError(f"[{data_validation_pipeline.__name__}] File type '{file_type}' is not recognized.")
    if file_type == "excel" and not sheet_name:
        raise ValueError(f"[{data_validation_pipeline.__name__}] sheet_name is required for excel file.")

    if df_processing_config is not None:
        df = process_data(df, df_processing_config, "preprocessing")
        if df is None:
            logger.warning(f"[{data_validation_pipeline.__name__}] Pre processing data is failed")
            return

    if df_validation_config is not None and not df_validation_config.empty:
        df = validate_data(df, df_validation_config)
        if df is None:
            logger.warning(f"[{data_validation_pipeline.__name__}] Validate data is failed")
            return
    validation_report_file_path: str = f"{report_folder_path}/{date_today}_{time_today}_{file_name}_validation_result.xlsx"
    df = process_result(df, file_path=validation_report_file_path, sheet_name=sheet_name)

    if df_processing_config is not None:
        df = process_data(df, df_processing_config, "postprocessing")
        if df is None:
            logger.warning(f"[{data_validation_pipeline.__name__}] Post processing data is failed")
            return
    processed_data_file_path: str = f"./data/processed_data/{date_today}/{date_today}_{time_today}_{file_name}.xlsx"
    write_data_strategy_factory.get_strategy(file_type)().run(df=df, file_path=processed_data_file_path, sheet_name=sheet_name)

    return df

            


    

