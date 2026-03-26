import pandas as pd
from typing import List
from loguru import logger
from utils.logger_wrapper import logger_wrapper

# @logger_wrapper
# def mark_result(
#     df: pd.DataFrame = None,
#     mask: pd.Series = None,
#     column: str = "",
#     validation_type: str = "",
#     message: str = ""
#     ) -> None:
#     if df is None or df.empty:
#         logger.warning("Data is empty.")
#         return
#     if mask is None or mask.empty:
#         logger.warning("Validation mask is empty.")
#         return
#     if column not in df.columns:
#         raise ValueError(f"{column} is not in data columns: {df.columns.values.tolist()}")
#     if not validation_type or not message:
#             logger.warning("validation_type, message are empty.")
#             return

#     error_message: str = f"[{column}] [{validation_type}] {message}."
#     df.loc[mask, "validation_result"] = df.loc[mask, "validation_result"].map(lambda x: x.union({error_message}))

#     error_amount: int = mask.sum()
#     if error_amount == 0:
#         logger.success(f"[{column}] [{validation_type}] All records are valid.")
#         return
    
#     shape: int = mask.shape[0]
#     sample_index_errors: List = (mask.where(mask).dropna().index + 2).values.tolist()[:5]
#     logger.error(f"[{column}] [{validation_type}] {message}. {error_amount}/{shape} records are invalid. Excel index example: {sample_index_errors}.")
#     return


@logger_wrapper
def mark_result(
    df: pd.DataFrame = None,
    mask: pd.Series = None,
    column: str = "",
    validation_type: str = "",
    message: str = "",
    extra_message: str = ""
    ) -> None:
    if df is None or df.empty:
        logger.warning("Data is empty.")
        return
    if mask is None or mask.empty:
        logger.warning("Validation mask is empty.")
        return
    # if column not in df.columns:
    #     raise ValueError(f"{column} is not in data columns: {df.columns.values.tolist()}")
    if not validation_type or not message:
            logger.warning("validation_type, message are empty.")
            return

    error_message: str = f"[{column}] [{validation_type}] {message}"
    # df.loc[mask, "validation_result"] = df.loc[mask, "validation_result"].map(lambda x: x.union({error_message}))
    failed_idx = df.index[mask]
    for idx in failed_idx:
        df.at[idx, "validation_result"].add(error_message)

    error_amount: int = mask.sum()
    if error_amount == 0:
        logger.success(f"[{column}] [{validation_type}] All records are valid. {extra_message}")
        return

    shape: int = mask.shape[0]
    sample_index_errors: List = (mask.where(mask).dropna().index + 2).values.tolist()[:5]
    logger.error(f"{error_message}. {error_amount}/{shape} records are invalid. Excel index example: {sample_index_errors}.")
    
    return

