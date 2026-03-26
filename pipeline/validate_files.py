import pandas as pd
from loguru import logger
from typing import Any, Callable
from pipeline.pipeline import validation_pipeline
from utils.logger_wrapper import logger_wrapper
from utils.logger import logger

@logger_wrapper
def validate_files(
    file_path: str,
    df_processing_config: pd.DataFrame,
    df_validation_config: pd.DataFrame,
    validation_sheet_set: set[str],
    *args,
    **kwargs
) -> dict[str, Any]:
    """
    Worker-safe function for one file.
    Each process handles one file.
    """
    # import psutil, os
    # proc = psutil.Process(os.getpid())
    # logger.info(f"[Worker] PID={proc.pid}, RSS at start = {proc.memory_info().rss / 1e6:.1f} MB")
    # logger.info(f"[Worker] System free RAM = {psutil.virtual_memory().available / 1e6:.1f} MB")
    # logger.info(f"[Worker] File: {file_path}")
    result = {
        "file_path": file_path,
        "status": "success",
        "validated_sheets": [],
        "skipped_sheets": [],
        "errors": [],
    }

    try:
        # # Read all sheets once (recommended for many-sheet workbooks)
        # workbook = pd.read_excel(
        #     file_path,
        #     sheet_name=None,
        #     dtype=str,
        #     engine="calamine",   # strongly recommended if installed
        # )
        sheet_names = pd.ExcelFile(file_path).sheet_names
        for sheet_name in sheet_names:
            if sheet_name not in validation_sheet_set:
                result["skipped_sheets"].append(sheet_name)
                continue
            additional_function = kwargs.get("additional_function").get(sheet_name)
            common_kwargs = kwargs.get("common_kwargs")
            try:
                # IMPORTANT:
                # your pipeline should support preloaded df to avoid re-reading
                validation_pipeline.run(
                    file_path=file_path,
                    sheet_name=sheet_name,
                    # df=df,  # <-- add this support
                    df_processing_config=df_processing_config,
                    df_validation_config=df_validation_config,
                    additional_function=additional_function,
                    **common_kwargs
                )
                result["validated_sheets"].append(sheet_name)

            except Exception as e:
                logger.exception(f"Error validating file={file_path}, sheet={sheet_name}")
                result["errors"].append({
                    "sheet_name": sheet_name,
                    "error": str(e),
                })

    except Exception as e:
        logger.exception(f"Error processing file={file_path}")
        result["status"] = "failed"
        result["errors"].append({
            "sheet_name": None,
            "error": str(e),
        })

    return result