import datetime
import pandas as pd
from loguru import logger
from .logger_wrapper import logger_wrapper


@logger_wrapper
def process_result(
    df: pd.DataFrame = None,
    file_path: str = "",
    sheet_name: str = "Sheet1",
    index: bool = False,
    *args,
    **kwargs
) -> None:
    """
    Process validation result and write to error report.
    """
    if df is None:
        raise ValueError(f"[{sheet_name}] df is required.")
    if df.empty:
        logger.success(
            f"[{sheet_name}] df is empty. No error needs recording."
        )
    if "validation_result" not in df.columns:
        raise ValueError(
            f"[{sheet_name}] validation_result is not in df columns: {df.columns.values.tolist()}]"
        )

    df_error = df.loc[df["validation_result"].map(lambda x: len(x) > 0)]
    df_error["datetime"] = datetime.datetime.strftime(datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=7))), format="%Y-%m-%d %H:%M:%S") # this is just for fun, if need assign time for report, assign it as: reports/date(YYYY-MM-DD)/<file_name>_<datetime: YYY-MM-DD_HH-MM-SS>.<file_type>, for example: reports/2026-03-09/learner_account_validation_report_2026-03-09_00-00-00.xlsx

    if df_error.empty:
        logger.success(f"[{sheet_name}] No error needs recording.")
    else:
        df_error["validation_result"] = df_error["validation_result"].map(
            lambda x: sorted(x)
        )
        df_error["validation_result"] = df_error["validation_result"].map(
            lambda x: "\n".join(x)
        )

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df_error.to_excel(
                writer, sheet_name=sheet_name, index=index, *args, **kwargs
            )

        logger.success(
            f"[{sheet_name}] Error report has been written to {file_path}"
        )

    mask = df["validation_result"].map(lambda x: len(x) == 0)
    df = df[mask].reset_index(drop=True)
    df = df.drop(columns="validation_result")
    return df
