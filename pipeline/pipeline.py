import pandas as pd
from loguru import logger
from typing import Callable, Optional
from utils.logger_wrapper import logger_wrapper
from utils.process_result import process_result
from utils.detect_file_type import detect_file_type
from configs.constants import datetime_today, date_today, report_folder_path
from .processing import process_data
from .validate import validate_data
from .setup import read_file_strategy_factory, write_data_strategy_factory
from helpers.factory import ReadFileStrategyFactory, WriteDataStrategyFactory

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


class ValidationPipeline:
    def __init__(
        self,
        read_file_strategy_factory: ReadFileStrategyFactory,
        write_data_strategy_factory: WriteDataStrategyFactory
    ) -> None:
        self.read_file_strategy_factory = read_file_strategy_factory
        self.write_data_strategy_factory = write_data_strategy_factory


    @logger_wrapper
    def read(
        self,
        file_path: str,
        sheet_name: str,
        file_type: str,
        *args,
        **kwargs
    ) -> Optional[pd.DataFrame]:
        return self.read_file_strategy_factory.get_strategy(file_type)(file_path).load(sheet_name, *args, **kwargs)
    
    @logger_wrapper
    def processing(
        self,
        df: pd.DataFrame,
        df_processing: pd.DataFrame,
        processing_action: str
    ) -> Optional[pd.DataFrame]:
        return process_data(df, df_processing, processing_action)

    @logger_wrapper
    def validate(self, df: pd.DataFrame, df_validation: pd.DataFrame, sheet_name):
        return validate_data(df, df_validation, sheet_name)

    @logger_wrapper
    def process_result(
        self,
        df,
        file_name,
        sheet_name,
        origin_file_path
    ):
        validation_report_file_path: str = f"{report_folder_path}/{datetime_today}/{file_name}_validation_result.xlsx"
        return process_result(
            df,
            file_path=validation_report_file_path,
            sheet_name=sheet_name,
            origin_file_path=origin_file_path
        )

    @logger_wrapper
    def write_data(self, df, file_name, sheet_name, file_type):
        processed_data_file_path: str = f"./data/processed_data/{date_today}/{datetime_today}/{file_name}.xlsx"
        self.write_data_strategy_factory.get_strategy(file_type)().run(
            df=df,
            file_path=processed_data_file_path,
            sheet_name=sheet_name
        )

    @logger_wrapper
    def run(
        self,
        file_path: str,
        sheet_name: Optional[str] = None,
        df_processing_config: Optional[pd.DataFrame] = None,
        df_validation_config: Optional[pd.DataFrame] = None,
        additional_function: Optional[Callable] = None,
        *args,
        **kwargs,
    ) -> Optional[pd.DataFrame]:

        if not file_path:
            raise ValueError(f"[{self.__class__.__name__}] file_path is required.")
        
        file_name: str = file_path.replace("\\", "/").split("/")[-1].split(".")[0] or f"{date_today}"
        file_type: str = detect_file_type(file_path)

        logger.info(f"[{self.__class__.__name__}] Validation for sheet: {sheet_name}, file: {file_path}")

        # Read data
        df = self.read(file_path, sheet_name, file_type)
        if df is None:
            logger.error(f"[{self.__class__.__name__}] Failed to read data.")
            return None
        
        if df.empty:
            logger.warning(f"[{self.__class__.__name__}] Input DataFrame is empty.")
            return df

        # Preprocessing
        df_preprocessing_config = pd.DataFrame()
        if df_processing_config is not None:
            df_preprocessing_config = df_processing_config.loc[(df_processing_config["sheet_name"] == sheet_name) & (df_processing_config["processing_action"] == "preprocessing")]
        df = self.processing(df, df_preprocessing_config, "preprocessing")
        if df is None:
            logger.warning(f"[{self.__class__.__name__}] Preprocessing failed.")
            return None

        # Validate
        df_sheet_validation_config = pd.DataFrame()
        if df_validation_config is not None:
            df_sheet_validation_config = df_validation_config.loc[df_validation_config["sheet_name"] == sheet_name]
        df = self.validate(df, df_sheet_validation_config, sheet_name)
        if df is None:
            logger.error(f"[{self.__class__.__name__}] Validation failed.")
            return None

        # Additional function to special logic processing
        if additional_function is not None:
            df = additional_function(df, *args, **kwargs)
            if df is None:
                logger.error(f"[{self.__class__.__name__}] Additional validation failed.")
                return None

        # Result processing (optional)
        df = self.process_result(
            df=df,
            file_name=file_name,
            sheet_name=sheet_name,
            origin_file_path=file_path
        )
        if df is None:
            logger.warning(f"[{self.__class__.__name__}] Result processing failed.")
            return None

        # # Postprocessing
        # df_postprocessing_config = pd.DataFrame()
        # if df_processing_config is not None:
        #     df_postprocessing_config = df_processing_config.loc[(df_processing_config["sheet_name"] == sheet_name) & (df_processing_config["processing_action"] == "postprocessing")]
        # df = self.processing(df, df_postprocessing_config, "postprocessing")
        # if df is None:
        #     logger.warning(f"[{self.__class__.__name__}] Postprocessing failed.")
        #     return None

        # # Write output
        # self.write_data(df, file_name, sheet_name, file_type)

        logger.info(f"[{self.__class__.__name__}] Complete validation for sheet: {sheet_name}, file: {file_path}")
        logger.info(f"{'-'*50}\n")

        return df

validation_pipeline = ValidationPipeline(read_file_strategy_factory, write_data_strategy_factory)

    

