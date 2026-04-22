import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_misc_item_currency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Only miscellaneous item that are setup as "Finance Setup > MiscellaneousItem > Currency" must have same "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency" will be available for selection.
    """
    incorrect_currency_mask: pd.Series = (
        df["Miscellaneous item name"].isin(df["Miscellaneous item name"])
    ) & ~(df["Currency"].isin(df["Misc currency"].unique()))
    mark_result(
        df,
        incorrect_currency_mask,
        column="Currency",
        validation_type="Special logic",
        message='Currency of misc item of course not the same as "Finance Setup > MiscellaneousItem > Currency" must have same "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency"',
        extra_message='All misc items of course have the same currency as "Finance Setup > MiscellaneousItem > Currency" must have same "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency"',
        sheet_name="CourseIntakeMiscellaneousItem",
    )

    return df


@logger_wrapper
def check_misc_eff_time(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    df["Intake start date"] = df["Intake start and end date"].map(
        lambda x: x.split(":")[0]
    )
    df["Intake start date"] = pd.to_datetime(
        df["Intake start date"], errors="coerce", format="%d%m%Y"
    )
    df["Effective to"] = pd.to_datetime(
        df["Effective to"], errors="coerce", format="%d-%m-%Y"
    )
    invalid_eff_time_mask: pd.Series = (df["Effective to"].notna()) & (
        df["Effective to"] < df["Intake start date"]
    )
    mark_result(
        df=df,
        mask=invalid_eff_time_mask,
        column="Effective to - Intake start date",
        validation_type="Special logic",
        message="Effective to of misc item of course must be after intake start date",
        extra_message="All misc items of course have effective to after intake start date",
        sheet_name="CourseIntakeMiscellaneousItem",
    )
    return df


@logger_wrapper
def check_charge_basics(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    3. Allow selection Charging basis follow this rule:
    a. "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public", only allow "Per pax"
    b. "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Corporate", allow "Per pax" or "Per intake"
    """
    public_intake_mask: pd.Series = df["Open to"] == "Public"
    per_pax_misc_mask: pd.Series = df["Charging basis"] == "Per pax"
    invalid_charge_code_mask: pd.Series = public_intake_mask & ~per_pax_misc_mask
    mark_result(
        df,
        invalid_charge_code_mask,
        column="Charging basis",
        validation_type="Special logic",
        message='If "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public", only allow "Charging basis" == "Per pax"',
        extra_message='All misc items of course with "Course IntakeSetupAndOthers > CourseIntake > Open to" = "Public" have "Charging basis" = "Per pax"',
        sheet_name="CourseIntakeMiscellaneousItem",
    )
    return df


@logger_wrapper
def course_intake_miscallenous_item(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Course intake
    df_intake: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntake",
        usecols=["CourseIntakeId", "Intake start and end date", "Open to"],
    )

    # Course intake fee setup
    df_intake_fee: pd.DataFrame = read_reference_data(
        intake_file_path, "CourseIntakeFeeSetup", usecols=["CourseIntakeId", "Currency"]
    )

    # Miscelleneous item
    df_misc_item = read_reference_data(
        finance_setup_file_path,
        "MiscellaneousItem",
        usecols=["Miscellaneous item name", "Currency"],
    )
    df_misc_item = df_misc_item.rename(columns={"Currency": "Misc currency"})

    # Miscellaneous eff time
    df_misc_item_eff_time = read_reference_data(
        finance_setup_file_path,
        "MiscellaneousItemEffectiveTime",
        usecols=["Miscellaneous item name", "Effective to"],
    )

    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    df_misc_item = df_misc_item.merge(
        df_misc_item_eff_time, how="left", on="Miscellaneous item name"
    )
    df_intake_fee = df_intake_fee.merge(df_intake, how="left", on="CourseIntakeId")
    df_check = df_check.merge(df_intake_fee, how="left", on="CourseIntakeId")
    df_check = df_check.merge(df_misc_item, how="left", on="Miscellaneous item name")

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # Not correct currency
    df_check = check_misc_item_currency(df_check)

    # effective to of misc < intake start date -> error
    df_check = check_misc_eff_time(df_check)

    # Charging basis
    df_check = check_charge_basics(df_check)

    df["validation_result"] = df_check["validation_result"]
    return df
