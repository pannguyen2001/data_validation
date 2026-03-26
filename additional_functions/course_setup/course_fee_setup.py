import pandas as pd
from typing import Optional
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_sfc(df: pd.DataFrame, tpg_course_mask: pd.Series) -> None:
    # Subsidy payment mode: SkillsFuture Credit (SFC) is only applicable if "Course Setup > Course > TPG Applicable" is "Yes"
    incorrect_sfc_mask: pd.Series = (df["CourseUniqueId"].isin(tpg_course_mask)) & (
        ~df["Subsidy payment mode"].str.contains(
            "SkillsFuture Credit (SFC)", regex=False, na=False
        )
    )

    mark_result(
        df=df,
        mask=incorrect_sfc_mask,
        column="Subsidy payment mode",
        validation_type="Special logic",
        message="SkillsFuture Credit (SFC) is only applicable if Course Setup > Course > TPG Applicable is Yes",
        extra_message="All non TPG courses don't have Subsidy payment mode = SkillsFuture Credit (SFC)",
    )

    return df


@logger_wrapper
def check_method_to_calculate(
    df: pd.DataFrame,
    df_course: pd.DataFrame,
    tpg_course_mask: pd.Series,
    funded_course_mask: pd.Series,
    blended_course_mask: pd.Series,
) -> None:
    # Method to calculate
    """
    1. Value list: Per Pax Attendees, Min. pax, Tier based, Lump sum, Per intake
    2. Allow selection Method to calculate follow this rule:
    a. If "Course Setup > Course > Funded course/TPG Applicable/Blended course" is "Yes", "Method to calculate" must be "Per Pax Attendees"
    b. If "Course Setup > Course > Funded course/TPG Applicable" is "No", "Method to calculate" allow all methods
    """
    fund_tpg_blended_courses: pd.Series = df_course.loc[
        tpg_course_mask | funded_course_mask | blended_course_mask,
        "CourseUniqueId",
    ].unique()
    incorrect_method_to_calculate_mask: pd.Series = (
        df["CourseUniqueId"].isin(fund_tpg_blended_courses)
    ) & (df["Method to calculate"] != "Per Pax Attendees")

    mark_result(
        df,
        incorrect_method_to_calculate_mask,
        column="Method to calculate",
        validation_type="Special logic",
        message='If Course Setup > Course > Funded course/TPG Applicable/Blended course = Yes, "Method to calculate" must be "Per Pax Attendees"',
        extra_message="All Funded course/TPG Applicable/Blended courses have Method to calculate = Per Pax Attendees",
    )

    return df


@logger_wrapper
def check_set_up_fee_at(
    df: pd.DataFrame,
    df_course: pd.DataFrame,
    tpg_course_mask: pd.Series,
    funded_course_mask: pd.Series,
    blended_course_mask: pd.Series,
    sctp_course_mask: pd.Series,
) -> pd.DataFrame:
    # Setup fee at
    """
    1. Value list: Course level, Module level
    2. Scenario:
    a.  Method to calculate is "Per Pax Attendees" and "Course Setup > Course > TPG Applicable" is "Yes" or "Couse Setup > Course > Blended course" is "Yes", "Setup fee at" must be "Course level"
    b. Method to calculate is "Per Pax Attendees" or "Min. pax", "Setup fee at" allow all: "Course level" or "Module level"
    c. Method to calculate is "Tier based', "Lump sum" or "Per intake", "Setup fee at" must be "Course level"
    d. If "Course Setup > Course > SCTP Course" is indicated as "Yes", "Setup fee at" must be "Module level"
    """
    course_level_fee_setup_at: pd.Series = df["Setup fee at"] == "Course level"
    module_level_fee_setup_at: pd.Series = df["Setup fee at"] == "Module level"

    # a. Method to calculate is "Per Pax Attendees" and "Course Setup > Course > TPG Applicable/Blended course" is "Yes", "Setup fee at" must be "Course level"
    tpg_blended_courses: pd.Series = df_course.loc[
        (tpg_course_mask | blended_course_mask), "CourseUniqueId"
    ].unique()
    incorrect_setup_fee_at_type_a_mask: pd.Series = (
        df["CourseUniqueId"].isin(tpg_blended_courses)
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_type_a_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message='If Method to calculate is "Per Pax Attendees" and "Course Setup > Course > TPG Applicable/Blended course" is "Yes", "Setup fee at" must be "Course level"',
        extra_message="All Funded course/TPG Applicable/Blended courses have Setup fee at = Course level",
    )

    # c. Method to calculate is "Tier based', "Lump sum" or "Per intake", "Setup fee at" must be "Course level"
    incorrect_setup_fee_at_type_c_mask: pd.Series = (
        df["Method to calculate"].isin(["Tier based", "Lump sum", "Per intake"])
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_type_c_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message='If Method to calculate is "Tier based", "Lump sum" or "Per intake", "Setup fee at" must be "Course level"',
        extra_message='All Method to calculate is "Tier based", "Lump sum" or "Per intake", "Setup fee at" = "Course level"',
    )

    # d. If "Course Setup > Course > SCTP Course" is indicated as "Yes", "Setup fee at" must be "Module level"
    incorrect_setup_fee_at_type_d_mask: pd.Series = (
        df["CourseUniqueId"].isin(sctp_course_mask)
    ) & (~module_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_type_d_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message='If "Course Setup > Course > SCTP Course" is indicated as "Yes", "Setup fee at" must be "Module level"',
        extra_message='All "Course Setup > Course > SCTP Course" = "Yes", "Setup fee at" = "Module level"',
    )

    return df


@logger_wrapper
def course_fee_setup(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    df_course = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=[
            "CourseUniqueId",
            "TPG Applicable",
            "SCTP course",
            "Blended course",
            "Funded course",
        ],
    )

    funded_course_mask: pd.Series = df_course["Funded course"] == "Yes"
    tpg_course_mask: pd.Series = df_course["TPG Applicable"] == "Yes"
    blended_course_mask: pd.Series = df_course["Blended course"] == "Yes"
    sctp_course_mask: pd.Series = df_course["SCTP course"] == "Yes"

    check_sfc(df, tpg_course_mask)

    check_method_to_calculate(
        df, df_course, tpg_course_mask, funded_course_mask, blended_course_mask
    )

    check_set_up_fee_at(
        df,
        df_course,
        tpg_course_mask,
        funded_course_mask,
        blended_course_mask,
        sctp_course_mask,
    )

    return df
