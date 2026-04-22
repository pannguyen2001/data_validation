import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def course_intake_fee_setup_discount(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
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
    df_discount = df_discount.rename(columns={"Currency": "Discount currency"})

    # CourseIntakeFeeSetup
    df_intake_fee_setup: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeFeeSetup",
        usecols=[
            "CourseIntakeId",
            "Method to calculate",
            "Currency"
        ]
    )

    # Course Intake
    df_intake: pd.Pandas = read_reference_data(
        intake_file_path,
        "CourseIntake",
        usecols=[
            "CourseIntakeId",
            "CourseUniqueId",
            "Open to"
        ]
    )

    # Course
    df_course: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=[
            "CourseUniqueId",
            "Funded course"
        ]
    )


    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    df_intake_fee_setup = df_intake_fee_setup.merge(df_intake, how="left", on="CourseIntakeId")
    df_intake_fee_setup = df_intake_fee_setup.merge(df_course, how="left", on="CourseUniqueId")
    df_check = df_check.merge(df_intake_fee_setup, how="left", on="CourseIntakeId")
    df_check = df_check.merge(df_discount, how="left", on="Discount name")


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # This sheet is only applicable when CourseIntakeFeeSetup > Method to calculate = "Per pax attendees"
    # -> check Method to calculate != "Per pax attendees" but still exist in this sheet
    invalid_per_pax_attendee_intake_mask: pd.Series = df_check["Method to calculate"] != "Per Pax Attendees"
    mark_result(
        df=df_check,
        mask=invalid_per_pax_attendee_intake_mask,
        column="Method to calculate",
        validation_type="Special logic",
        message='This sheet is only applicable when CourseIntakeFeeSetup > Method to calculate = "Per pax attendees"',
        extra_message='All courses have Method to calculate = "Per pax attendees"',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # # -> check Method to calculate = "Per pax attendees" but not exist in this sheet
    # missing_intake_ids: pd.Series = df_intake_fee_setup.loc[~df_intake_fee_setup["CourseIntakeId"].isin(df["CourseIntakeId"]), "CourseIntakeId"].unique()
    # if not missing_intake_ids.empty:
    #     logger.error(f"[CourseIntakeFeeSetupDiscount- CourseIntakeId] [Special logic] Some courses in course intake fee setup have Method to calculate = Per pax attendees but not in course intake fee setup discount. Amount: {len(missing_intake_ids)}. Details: {missing_intake_ids}")
    # else:
    #     logger.success("[CourseIntakeFeeSetupDiscount - CourseIntakeId] [Special logic] All courses in course intake fee setup have Method to calculate = Per pax attendees are in course intake fee setup discount")

    # Discount item that are setup as "Finance Setup > Discount > Currency" must have same "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency"
    invalid_currency_mask: pd.Series = (
        (~df_check["Discount currency"].isin(df_check["Currency"].unique()))
    )
    mark_result(
        df=df_check,
        mask=invalid_currency_mask,
        column="Discount name",
        validation_type="Special logic",
        message='Discount item that are setup as "Finance Setup > Discount > Currency" must have same "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency"',
        extra_message='All discount items have the same currency as "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Currency"',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # "Finance Setup > Discount > Discount applied to" is "Per Company" only applicable if "Course Setup> Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Corporate" -> meaning that if not funded course, open to = corporate, just fill discount per company, discount per learner is not applicable
    company_discount_mask: pd.Series = df_check["Discount applied to"] == "Per Company"
    funded_course_mask: pd.Series = df_check["Funded course"] == "No"
    corporate_intake_mask: pd.Series = df_check["Open to"] == "Corporate"
    invalid_discount_applied_to_mask: pd.Series = (
        funded_course_mask
        & corporate_intake_mask
        & ~company_discount_mask
    )
    mark_result(
        df=df_check,
        mask=invalid_discount_applied_to_mask,
        column="Discount name",
        validation_type="Special logic",
        message='If "Course Setup> Course > Funded course" = "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" = "Corporate", just fill discount has "Finance Setup > Discount > Discount applied to" = "Per Company"',
        extra_message='All discount items have Discount applied to = Per Company for intake has Funded course = No and Open to = Corporate',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    #  If "Finance Setup > Discount > Discount applied to" is "Per Company", "Applicable for sales location?" must be "No"
    invalid_applicable_for_sales_location_mask: pd.Series = (
        company_discount_mask
        & (df_check["Applicable for sales location?"] == "Yes")
    )
    mark_result(
        df=df_check,
        mask=invalid_applicable_for_sales_location_mask,
        column="Applicable for sales location?",
        validation_type="Special logic",
        message='If "Finance Setup > Discount > Discount applied to" is "Per Company", "Applicable for sales location?" must be "No"',
        extra_message='All discount items have Discount applied to = Per Company and Applicable for sales location = No',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # Effective from is mandatory if "Finance Setup > Discount > Discount applied to" is "Per Learner"
    per_learner_discount_mask: pd.Series = df_check["Discount applied to"] == "Per Learner"
    invalid_effective_from_mask: pd.Series = (
        per_learner_discount_mask
        & (df_check["Effective from"].isna())
    )
    mark_result(
        df=df_check,
        mask=invalid_effective_from_mask,
        column="Effective from",
        validation_type="Check mandatory",
        message='Effective from is mandatory if "Finance Setup > Discount > Discount applied to" is "Per Learner"',
        extra_message='All discount items have Discount applied to = Per Learner and Effective from is not empty',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # Effective to is mandatory if "Finance Setup > Discount > Discount applied to" is "Per Learner"
    invalid_effective_to_mask: pd.Series = (
        per_learner_discount_mask
        & (df_check["Effective to"].isna())
    )
    mark_result(
        df=df_check,
        mask=invalid_effective_to_mask,
        column="Effective to",
        validation_type="Check mandatory",
        message='Effective to is mandatory if "Finance Setup > Discount > Discount applied to" is "Per Learner"',
        extra_message='All discount items have Discount applied to = Per Learner and Effective to is not empty',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # Period for discount to be applied on registrations and bookings is not mandatory but just filled if "Finance Setup > Discount > Discount applied to" is "Per Learner"
    invalid_period_for_discount_mask: pd.Series = (
        company_discount_mask
        & (df_check["Period for discount to be applied on registrations and bookings"].notna())
    )
    mark_result(
        df=df_check,
        mask=invalid_period_for_discount_mask,
        column="Period for discount to be applied on registrations and bookings",
        validation_type="Special logic",
        message='Period for discount to be applied on registrations and bookings is not mandatory but just filled if "Finance Setup > Discount > Discount applied to" is "Per Learner"',
        extra_message='All discount items have Discount applied to = Per Learner and Period for discount to be applied on registrations and bookings is not empty',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    # Discount available to first X learners is not mandatory but just filled if "Finance Setup > Discount > Discount applied to" is "Per Learner"
    invalid_discount_available_mask: pd.Series = (
        company_discount_mask
        & (df_check["Discount available to first X learners"].notna())
    )
    mark_result(
        df=df_check,
        mask=invalid_discount_available_mask,
        column="Discount available to first X learners",
        validation_type="Special logic",
        message='Discount available to first X learners is not mandatory but just filled if "Finance Setup > Discount > Discount applied to" is "Per Learner"',
        extra_message='All discount items have Discount applied to = Per Learner and Discount available to first X learners is not empty',
        sheet_name="CourseIntakeFeeSetupDiscount"
    )

    df["validation_result"] = df_check["validation_result"]
    return df