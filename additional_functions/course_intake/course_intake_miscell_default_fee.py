import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def course_intake_miscell_default_fee(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Course intake misc item
    df_intake_misc_item: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeMiscellaneousItem",
    )

    # Miscelleneous item
    df_misc_item = read_reference_data(
        finance_setup_file_path,
        "MiscellaneousItem",
        usecols=["Miscellaneous item name", "Editable", "Currency"],
    )
    df_misc_item = df_misc_item.rename(columns={"Currency": "Misc currency"})

    # Miscellaneous eff time
    df_misc_item_eff_time = read_reference_data(
        finance_setup_file_path,
        "MiscellaneousItemEffectiveTime",
    )
    df_misc_item_eff_time = df_misc_item_eff_time.rename(
        columns={
            "Effective from": "Misc effective from",
            "Effective to": "Misc effective to",
            "Default Fee before GST": "Misc fee before GST",
        }
    )

    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    df_misc_item = df_misc_item.merge(
        df_misc_item_eff_time, how="left", on="Miscellaneous item name"
    )
    df_intake_misc_item = df_intake_misc_item.merge(
        df_misc_item, how="left", on="Miscellaneous item name"
    )
    df_check = df_check.merge(
        df_intake_misc_item,
        how="left",
        on=["CourseIntakeId", "Miscellaneous item name"],
    )

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # "On top of" misc in CourseIntakeMiscellaneousItem but not in CourseIntakeMiscellDefaultFee
    intake_misc_item_not_in_default_fee_mask: pd.Series = (
        ~df_intake_misc_item["CourseIntakeId"].isin(df_check["CourseIntakeId"].unique())
    ) & (df_intake_misc_item["Miscellaneous type"] == "On top of")
    intake_misc_item_not_in_default_fee_values: pd.Series = df_intake_misc_item.loc[
        intake_misc_item_not_in_default_fee_mask,
        ["CourseIntakeId", "Miscellaneous item name"],
    ]
    if not intake_misc_item_not_in_default_fee_values.empty:
        logger.error(
            f"[CourseIntakeMiscellDefaultFee - CourseIntakeId] [Count value] Some CourseIntakeMiscellaneousItem do not in CourseIntakeMiscellDefaultFee. Amount: {intake_misc_item_not_in_default_fee_values.shape[0]}. Detail: {intake_misc_item_not_in_default_fee_values.to_dict(orient='records')}"
        )
    else:
        logger.success(
            "[CourseIntakeMiscellDefaultFee - CourseIntakeId] [Count value] All data in CourseIntakeMiscellaneousItem is in CourseIntakeMiscellDefaultFee."
        )

    # Effective from: Automatically inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Effective from" and not editable at Course intake level.
    invalid_misc_eff_from_mask: pd.Series = (
        df_check["Misc effective from"].astype(str).str.strip() != df_check["Effective from"].astype(str).str.strip()
    )
    mark_result(
        df=df_check,
        mask=invalid_misc_eff_from_mask,
        column="Effective from",
        validation_type="Special logic",
        message='Effective from: Automatically inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Effective from" and not editable at Course intake level',
        sheet_name="CourseIntakeMiscellDefaultFee",
        extra_message="All Misc Effective from = Effective from",
    )

    # Effective to Automatically inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Effective to" and not editable at Course intake level.
    invalid_misc_eff_to_mask: pd.Series = (
        (df_check["Misc effective to"] != df_check["Effective to"])
        & (df_check["Effective to"].notna())
    )
    mark_result(
        df=df_check,
        mask=invalid_misc_eff_to_mask,
        column="Effective to",
        validation_type="Special logic",
        message='Effective to: Automatically inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Effective to" and not editable at Course intake level',
        sheet_name="CourseIntakeMiscellDefaultFee",
        extra_message="All Misc Effective to = Effective to",
    )
    # inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Default Fee before GST". If "Finance Setup > MiscellaneousItem > Editable" is "Yes", editable at Course Intake level -> if Editable = no -> can not edit
    invalid_misc_fee_mask: pd.Series = (
        df_check["Misc fee before GST"] != df_check["Default fee before GST"]
    ) & (df_check["Editable"] == "No")
    mark_result(
        df=df_check,
        mask=invalid_misc_fee_mask,
        column="Default Fee before GST",
        validation_type="Special logic",
        message='Default Fee before GST inherited from "Finance Setup > MiscellaneousItemEffectiveTime > Default Fee before GST if Editable = no',
        sheet_name="CourseIntakeMiscellDefaultFee",
        extra_message="All Misc fee before GST = Default Fee before GST",
    )

    df["validation_result"] = df_check["validation_result"]
    return df
