import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_sfc(df: pd.DataFrame, tpg_course: pd.Series) -> None:
    # Subsidy payment mode: SkillsFuture Credit (SFC) is only applicable if "Course Setup > Course > TPG Applicable" is "Yes" -> Not TPG course, not empty and contain SFC -> fail
    incorrect_sfc_mask: pd.Series = (~df["CourseUniqueId"].isin(tpg_course)) & (
        df["Subsidy payment mode"].str.contains(
            "SkillsFuture Credit (SFC)", regex=False, na=False
        )
        & (df["Subsidy payment mode"].notna())
    )

    mark_result(
        df=df,
        mask=incorrect_sfc_mask,
        column="Subsidy payment mode",
        validation_type="Special logic",
        message="SkillsFuture Credit (SFC) is only applicable if Course Setup > Course > TPG Applicable is Yes",
        extra_message="All non TPG courses don't have Subsidy payment mode = SkillsFuture Credit (SFC)",
        sheet_name="CourseFeeSetup",
    )

    return df


@logger_wrapper
def check_method_to_calculate(
    df: pd.DataFrame,
    df_course: pd.DataFrame,
    tpg_course: pd.Series,
    funded_course: pd.Series,
    blended_course: pd.Series,
) -> None:
    # Method to calculate
    """
    1. Value list: Per Pax Attendees, Min. pax, Tier based, Lump sum, Per intake
    2. Allow selection Method to calculate follow this rule:
    a. If "Course Setup > Course > Funded course/TPG Applicable/Blended course" is "Yes", "Method to calculate" must be "Per Pax Attendees"
    b. If "Course Setup > Course > Funded course/TPG Applicable" is "No", "Method to calculate" allow all methods
    """
    fund_tpg_blended_courses: set = tpg_course & funded_course & blended_course
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
        sheet_name="CourseFeeSetup",
    )

    return df


@logger_wrapper
def check_set_up_fee_at(
    df: pd.DataFrame,
    df_course: pd.DataFrame,
    tpg_course: set,
    funded_course: set,
    blended_course: set,
    sctp_modularised_course: set,
    sctp_non_modularised_course: set,
    sctp_as_normal_tpg_course: set,
    normal_tpg_course: set,
) -> pd.DataFrame:
    # Setup fee at
    """
    1. Value list: Course level, Module level
    2. Scenario:
        1. TPG Applicable = Yes
            - 1.1. SCTP course = Yes:
                - 1.1.1. SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level
                - 1.1.2. SCTP type = Non-Modularised SCTP course ->  Setup fee at: Course level or Module level
                - 1.1.3. SCTP type = SCTP as normal TPG course -> Setup fee at: Course level
            - 1.2. SCTP course = No:
                - 1.2.1. SCTP type = Normal TPG course -> Setup fee at: Course level
            - 1.3. SCTP course = empty:
                - 1.3.1. Method to calculate = Per Pax Attendees -> Setup fee at: Course level
        2. TPG Applicable = No, Blended course = Yes
            - 2.1. Method to calculate = Per Pax Attendees -> Setup fee at: Course level
        3. TPG Applicable = No, Blended course = No
            - 3.1. Method to calculate = Per Pax Attendees, Min. pax -> Setup fee at: Course level or Module level
            - 3.2. Method to calculate = Tier based, Lump sum, Per intake -> Setup fee at: Course level
    """
    course_level_fee_setup_at: pd.Series = df["Setup fee at"] == "Course level"
    module_level_fee_setup_at: pd.Series = df["Setup fee at"] == "Module level"

    # 1. TPG Applicable = Yes
    # - 1.1. SCTP course = Yes:
    # - 1.1.1. SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level
    incorrect_setup_fee_at_modularised_sctp_mask: pd.Series = (
        df["CourseUniqueId"].isin(sctp_modularised_course)
    ) & (~module_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_modularised_sctp_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="If SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level",
        extra_message="All SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level",
        sheet_name="CourseFeeSetup",
    )

    # 1. TPG Applicable = Yes
    # - 1.1. SCTP course = Yes:
    # - 1.1.2. SCTP type = Non-Modularised SCTP course ->  Setup fee at: Course level or Module level
    # No need check, just 2 values: course level and module level, if incorrect, it is type: invalid value

    # 1. TPG Applicable = Yes
    # - 1.1. SCTP course = Yes:
    # - 1.1.3. SCTP type = SCTP as normal TPG course -> Setup fee at: Course level
    incorrect_setup_fee_at_sctp_as_tpg_mask: pd.Series = (
        df["CourseUniqueId"].isin(sctp_as_normal_tpg_course)
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_sctp_as_tpg_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="If SCTP type = SCTP as normal TPG course -> Setup fee at: Course level",
        extra_message="All SCTP type = SCTP as normal TPG course -> Setup fee at: Course level",
        sheet_name="CourseFeeSetup",
    )

    # 1. TPG Applicable = Yes
    # - 1.2. SCTP course = No:
    # - 1.2.1. SCTP type = Normal TPG course -> Setup fee at: Course level
    incorrect_setup_fee_at_normal_tpg_mask: pd.Series = (
        df["CourseUniqueId"].isin(normal_tpg_course)
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_normal_tpg_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="If SCTP type = Normal TPG course -> Setup fee at: Course level",
        extra_message="All SCTP type = Normal TPG course -> Setup fee at: Course level",
        sheet_name="CourseFeeSetup",
    )

    #   1. TPG Applicable = Yes
    # - 1.3. SCTP course = empty/no:
    # - 1.3.1. SCTP type is not Normal TPG course, Method to calculate = Per Pax Attendees -> Setup fee at: Course level
    tpg_course_only: set = df_course.loc[
        (df_course["TPG Applicable"] == "Yes")
        & (
            df_course["SCTP course"].isna()
            | (~df["CourseUniqueId"].isin(normal_tpg_course)
                & (df_course["SCTP course"] == "No")
            )
        )
        ,
        "CourseUniqueId",
    ]
    incorrect_tpg_course_set_up_fee_at_mask: pd.Series = (
        df["CourseUniqueId"].isin(tpg_course_only)
        & (df["Method to calculate"] == "Per Pax Attendees")
        & (~course_level_fee_setup_at)
    )
    mark_result(
        df,
        incorrect_tpg_course_set_up_fee_at_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message='If TPG Applicable = Yes, SCTP course = empty/no, SCTP type is not Normal TPG course, "Method to calculate" = Per Pax Attendees, "Setup fee at" = Course level',
        extra_message='All TPG Applicable = Yes, SCTP course = empty/no, SCTP type is not Normal TPG course, "Method to calculate" = Per Pax Attendees, "Setup fee at" = Course level',
        sheet_name="CourseFeeSetup",
    )

    # 2. TPG Applicable = No, Blended course = Yes
    # - 2.1. Method to calculate = Per Pax Attendees -> Setup fee at: Course level
    # -> Incorrect if not course level
    incorrect_setup_fee_at_type_a_mask: pd.Series = (
        (~df["CourseUniqueId"].isin(tpg_course))
        & (df["CourseUniqueId"].isin(blended_course))
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_type_a_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="TPG Applicable = No, Blended course = Yes,Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        extra_message="Correct data: TPG Applicable = No, Blended course = Yes,Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        sheet_name="CourseFeeSetup",
    )

    # TPG Applicable = No, Blended course = No
    # - 3.1. Method to calculate = Per Pax Attendees, Min. pax -> Setup fee at: Course level or Module level
    # No need check, just 2 values: course level and module level, if incorrect, it is type: invalid value

    # TPG Applicable = No, Blended course = No
    # - 3.2. Method to calculate = Tier based, Lump sum, Per intake -> Setup fee at: Course level
    incorrect_setup_fee_at_type_c_mask: pd.Series = (
        (~df["CourseUniqueId"].isin(tpg_course))
        & (~df["CourseUniqueId"].isin(blended_course))
        & (df["Method to calculate"].isin(["Tier based", "Lump sum", "Per intake"]))
    ) & (~course_level_fee_setup_at)
    mark_result(
        df,
        incorrect_setup_fee_at_type_c_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message='If Method to calculate is "Tier based", "Lump sum" or "Per intake", "Setup fee at" must be "Course level"',
        extra_message='All Method to calculate is "Tier based", "Lump sum" or "Per intake", "Setup fee at" = "Course level"',
        sheet_name="CourseFeeSetup",
    )

    return df


@logger_wrapper
def course_fee_setup(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Course
    df_course = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=[
            "CourseUniqueId",
            "TPG Applicable",
            "SCTP course",
            "Blended course",
            "Funded course",
            "SCTP type",
        ],
    )

    funded_course = set(
        df_course.loc[df_course["Funded course"] == "Yes", "CourseUniqueId"]
    )
    tpg_course = set(
        df_course.loc[df_course["TPG Applicable"] == "Yes", "CourseUniqueId"]
    )
    blended_course = set(
        df_course.loc[df_course["Blended course"] == "Yes", "CourseUniqueId"]
    )
    sctp_modularised_course = set(
        df_course.loc[
            (df_course["SCTP type"] == "Modularised bundle SCTP course")
            & (df_course["SCTP course"] == "Yes"),
            "CourseUniqueId",
        ]
    )
    sctp_non_modularised_course = set(
        df_course.loc[
            (df_course["SCTP type"] == "Non-Modularised SCTP course")
            & (df_course["SCTP course"] == "Yes"),
            "CourseUniqueId",
        ]
    )
    sctp_as_normal_tpg_course = set(
        df_course.loc[
            (df_course["SCTP type"] == "SCTP as normal TPG course")
            & (df_course["SCTP course"] == "Yes"),
            "CourseUniqueId",
        ]
    )
    normal_tpg_course = set(
        df_course.loc[df_course["SCTP type"] == "Normal TPG course", "CourseUniqueId"]
    )

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # Check course in course file but not in course fee setup file:
    course_ids: pd.Series = set(df_course["CourseUniqueId"].unique())
    course_fee_setup_ids: pd.Series = set(df["CourseUniqueId"].unique())
    course_ids_not_in_course_fee_setup: set = course_ids - course_fee_setup_ids
    # logger.info(f"{course_ids_not_in_course_fee_setup = }")
    if len(course_ids_not_in_course_fee_setup) > 0:
        logger.error(
            f"[CourseFeeSetup - CourseUniqueId] [Special logic] Some courses do not have CourseFeeSetup. Amount: {len(course_ids_not_in_course_fee_setup)}. Details: {course_ids_not_in_course_fee_setup}"
        )
    else:
        logger.success(
            "[CourseFeeSetup - CourseUniqueId] [Special logic] All courses have CourseFee setup"
        )

    id_not_in_course_mask: pd.Series = ~df["CourseUniqueId"].isin(
        df_course["CourseUniqueId"]
    )
    mark_result(
        df,
        id_not_in_course_mask,
        column="CourseUniqueId",
        validation_type="Special logic",
        message="CourseUniqueId not in Course file",
        sheet_name="CourseFeeSetup",
        extra_message="All CourseUniqueId in CourseFeeSetup file",
    )

    check_sfc(df, tpg_course)

    check_method_to_calculate(df, df_course, tpg_course, funded_course, blended_course)

    check_set_up_fee_at(
        df=df,
        df_course=df_course,
        tpg_course=tpg_course,
        funded_course=funded_course,
        blended_course=blended_course,
        sctp_modularised_course=sctp_modularised_course,
        sctp_non_modularised_course=sctp_non_modularised_course,
        sctp_as_normal_tpg_course=sctp_as_normal_tpg_course,
        normal_tpg_course=normal_tpg_course,
    )

    return df
