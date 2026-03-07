import pandas as pd
from loguru import logger
from .logger_wrapper import logger_wrapper


@logger_wrapper
def process_result(
    df: pd.DataFrame = None,
    file_path: str = "",
    sheet_name: str = "",
    *args,
    **kwargs
    ) -> None:
    """
    Process validation result and write to error report.
    """
    if df is None:
        raise ValueError(f"[{process_result.__name__}] df is required.")
    if df.empty:
        logger.success(f"[{process_result.__name__}] df is empty. No error needs recording.")
    if "validation_result" not in df.columns:
        raise ValueError(f"[{process_result.__name__}] validation_result is not in df columns: {df.columns.values.tolist()}]")

    df_error = df.loc[df["validation_result"].map(lambda x: len(x) > 0)]

    if df_error.empty:
        logger.success(f"[{process_result.__name__}] No error needs recording.")

    df_error["validation_result"] = df_error["validation_result"].map(lambda x: sorted(x))
    df_error["validation_result"] = df_error["validation_result"].map(lambda x: "\n".join(x))

    with pd.ExcelWriter(file_path) as writer:
        df_error.to_excel(writer, sheet_name= sheet_name, *args, **kwargs)