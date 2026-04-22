import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def honorarium_batch(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    finance_process_file_path: str = kwargs.get("${FINANCE_PROCESS_FILE_PATH}")


    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # HonorariumRecord
    df_honorarium_record: pd.DataFrame = read_reference_data(
        finance_process_file_path,
        "HonorariumRecord",
        usecols=[
            "Honorarium batch",
            "Amount"
        ],
    )

    # ----------------------------------------
    # Process data
    # ----------------------------------------
    df_honorarium_record["Amount"] = df_honorarium_record["Amount"].astype(
        float).round(4).fillna(0)
    df_honorarium_record = df_honorarium_record.groupby("Honorarium batch").agg({"Amount": "sum"}).reset_index()
    df_honorarium_record = df_honorarium_record.rename(columns={"Honorarium batch": "Honorarium batch ID","Amount": "Honorarium amount"})

    df_check["Total amount"] = df_check["Total amount"].astype(
        float).round(4).fillna(0)
    df_check = df_check.merge(df_honorarium_record, how="left",
                        on="Honorarium batch ID")
    df_check["Honorarium amount"] = df_check["Honorarium amount"].astype(float).round(4).fillna(0)

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # Honorarium batch in HonorariumRecord but not in honorium batch sheet
    missing_hono_batch_mask: pd.Series = ~df_honorarium_record["Honorarium batch ID"].isin(df_check["Honorarium batch ID"].unique())
    missing_hono_batch_ids: pd.Series = df_honorarium_record.loc[missing_hono_batch_mask, "Honorarium batch ID"].unique()
    if len(missing_hono_batch_ids) > 0:
        logger.error(
            f"[HonorariumBatch] [Honorarium batch ID] Honorarium batch in HonorariumRecord but not in honorium batch sheet. Amount: {len(missing_hono_batch_ids)}. Details:\n{missing_hono_batch_ids.to_markdown()}.")
    else:
        logger.success(
            "[HonorariumBatch] [Honorarium batch ID] Honorarium batch in HonorariumRecord and in honorium batch sheet.")

    # Incorrect amount
    incorrect_amount_mask: pd.Series = df_check["Total amount"] != df_check["Honorarium amount"]
    mark_result(
        df=df_check,
        mask=incorrect_amount_mask,
        validation_type="Special logic",
        column="Total amount",
        message="Total amount is not equal to sum of Honorarium amount that have the same Honorarium batch ID",
        extra_message="Total amount is equal to sum of Honorarium amount that have the same Honorarium batch ID",
        sheet_name="HonorariumBatch"
    )

    df["validation_result"] = df_check["validation_result"]

    return df
