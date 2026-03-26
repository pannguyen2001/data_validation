import pandas as pd
from .logger_wrapper import logger_wrapper


@logger_wrapper
def build_validation_sheet_set(df_validation: pd.DataFrame) -> set[str]:
    if df_validation is None or df_validation.empty or "sheet_name" not in df_validation.columns:
        return set()

    return set(df_validation["sheet_name"].dropna().astype(str).unique())