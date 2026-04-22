import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_discount(df: pd.DataFrame, df_discount: pd.DataFrame) -> None:
    """
    4. Only discount item that are setup as "Finance Setup > Discount > Discount applied to"  is "Per Learner" and "Finance Setup > Discount > Currency" must have same "Course Setup > CourseFeeSetup > Currency" will be available for selection.
    """
    incorrect_currency_mask: pd.Series = (
        (df["Discount name"].isin(df_discount["Discount name"].unique()))
        & ~(df["Currency"].isin(df_discount["Currency"].unique()))
    )
    mark_result(
        df,
        incorrect_currency_mask,
        column="Currency",
        validation_type="Special logic",
        message='Currency of misc item of course not the same as "Finance Setup > MiscellaneousItem > Currency" must have same ',
        extra_message='All misc items of course have the same currency as "Finance Setup > MiscellaneousItem > Currency" must have same ',
        sheet_name="CourseFeeSetupDiscount"
    )

    return df


@logger_wrapper
def course_fee_setup_discount(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Discount
    df_discount = read_reference_data(
        finance_setup_file_path,
        "Discount",
        usecols=[
            "Discount name",
            "Discount applied to",
            "Currency"
        ]
    )
    df_discount = df_discount.loc[df_discount["Discount applied to"] == "Per Learner"]

    # Course fee setup
    df_course_fee_setup = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=[
            "CourseUniqueId",
            "Method to calculate",
            "Currency",
        ]
    )


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    df_course_fee_setup = df_course_fee_setup.loc[
        df_course_fee_setup["CourseUniqueId"].isin(df_check["CourseUniqueId"])]
    df_check = df_check.merge(df_course_fee_setup, on="CourseUniqueId", how="left")

    # This sheet is only applicable when CourseFeeSetup > Method to calculate = "Per pax attendees"
    # -> check Method to calculate != "Per pax attendees" but still exist in this sheet
    not_per_pax_attendee_course: pd.Series = df_course_fee_setup.loc[df_course_fee_setup["Method to calculate"] != "Per Pax Attendees", "CourseUniqueId"].unique()
    invalid_courses_mask: pd.Series = df_check["CourseUniqueId"].isin(not_per_pax_attendee_course)
    mark_result(
        df_check,
        invalid_courses_mask,
        column="CourseUniqueId",
        validation_type="Special logic",
        message='This sheet is only applicable when CourseFeeSetup > Method to calculate = "Per pax attendees"',
        extra_message='All courses have Method to calculate = "Per pax attendees"',
        sheet_name="CourseFeeSetupDiscount"
    )

    df_check = check_discount(df_check, df_discount)
    df["validation_result"] = df_check["validation_result"]

    return df

    # # -> check Method to calculate = "Per pax attendees" but not exist in this sheet
    # per_pax_attendee_course: pd.Series = df_course_fee_setup.loc[df_course_fee_setup["Method to calculate"] == "Per Pax Attendees", "CourseUniqueId"].unique()
    # not_in_course_fee_setup_discount = set(per_pax_attendee_course) - set(df["CourseUniqueId"])
    # if len(not_in_course_fee_setup_discount) > 0:
    #     logger.error(f"[CourseFeeSetupDiscount - CourseUniqueId] [Special logic] Some courses in course fee setup have Method to calculate = Per pax attendees but not in course setup discount. Amount: {len(not_in_course_fee_setup_discount)}. Details: {not_in_course_fee_setup_discount}")
    # else:
    #     logger.success("[CourseFeeSetupDiscount - CourseUniqueId] [Special logic] All courses in course fee setup have Method to calculate = Per pax attendees are in course setup discount")
