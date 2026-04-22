import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_method_to_calculate(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    1. Value list: Per Pax Attendees, Min. pax, Tier based, Lump sum, Per intake 
    2. Allow selection Method to calculate follow this rule: 
    a. "Course Setup > Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public", "Method to calculate" must be "Per Pax Attendees"
    b. "Course Setup > Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Corporate", "Method to calculate" allow all methods
    c. "Course Setup > Course > Funded course" is "Yes" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public" or "Corporate", "Method to calculate" must be "Per Pax Attendees"
    d. "Couse Setup > Course > Blended course" is "Yes", "Method to calculate" must be "Per Pax Attendees"
    """
    blended_course_mask: pd.Series = df["Blended course"] == "Yes"
    funded_course_mask: pd.Series = df["Funded course"] == "Yes"

    public_course_mask: pd.Series = df["Open to"] == "Public"
    per_pax_attendees_course_mask: pd.Series = df["Method to calculate"] == "Per Pax Attendees"
    # a. "Course Setup > Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public", "Method to calculate" must be "Per Pax Attendees"
    invalid_method_to_calculate_mask_1: pd.Series = (
        ~funded_course_mask
        & public_course_mask
        & ~per_pax_attendees_course_mask
    )

    mark_result(
        df=df,
        mask=invalid_method_to_calculate_mask_1,
        column="Method to calculate",
        validation_type="Special logic",
        message='"Course Setup > Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public", "Method to calculate" must be "Per Pax Attendees"',
        extra_message='All "Course Setup > Course > Funded course" = "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" = "Public" have "Method to calculate" = "Per Pax Attendees"',
        sheet_name="CourseIntakeFeeSetup",
    )

    # b. "Course Setup > Course > Funded course" is "No" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Corporate", "Method to calculate" allow all methods
    #  No need to validate here, if invalid value, it will be validation type: invalid value

    # c. "Course Setup > Course > Funded course" is "Yes" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public" or "Corporate", "Method to calculate" must be "Per Pax Attendees"
    invalid_method_to_calculate_mask_3: pd.Series = (
        funded_course_mask
        & ~per_pax_attendees_course_mask
    )
    mark_result(
        df=df,
        mask=invalid_method_to_calculate_mask_3,
        column="Method to calculate",
        validation_type="Special logic",
        message='"Course Setup > Course > Funded course" is "Yes" and "Course IntakeSetupAndOthers > CourseIntake > Open to" is "Public" or "Corporate", "Method to calculate" must be "Per Pax Attendees"',
        extra_message='All "Course Setup > Course > Funded course" = "Yes" and "Course IntakeSetupAndOthers > CourseIntake > Open to" = "Public" or "Corporate" have "Method to calculate" = "Per Pax Attendees"',
        sheet_name="CourseIntakeFeeSetup",
    )

    # d. "Couse Setup > Course > Blended course" is "Yes", "Method to calculate" must be "Per Pax Attendees"
    invalid_method_to_calculate_mask_4: pd.Series = (
        blended_course_mask & ~per_pax_attendees_course_mask
    )
    mark_result(
        df=df,
        mask=invalid_method_to_calculate_mask_4,
        column="Method to calculate",
        validation_type="Special logic",
        message='"Couse Setup > Course > Blended course" is "Yes", "Method to calculate" must be "Per Pax Attendees"',
        extra_message='All "Couse Setup > Course > Blended course" = "Yes" have "Method to calculate" = "Per Pax Attendees"',
        sheet_name="CourseIntakeFeeSetup",
    )

    return df


@logger_wrapper
def check_setup_fee_logic(df: pd.DataFrame) -> Optional[pd.DataFrame]:
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
    blended_course_mask: pd.Series = df["Blended course"] == "Yes"
    tpg_applicable_mask: pd.Series = df["TPG Applicable"] == "Yes"
    sctp_course_mask: pd.Series = df["SCTP course"] == "Yes"

    modularised_bundle_sctp_course_mask: pd.Series = df["SCTP type"] == "Modularised bundle SCTP course"
    sctp_as_normal_tpg_course_mask: pd.Series = df["SCTP type"] == "SCTP as normal TGP course"
    normal_tpg_course_mask: pd.Series = df["SCTP type"] == "Normal TPG course"

    course_level_mask: pd.Series = df["Setup fee at"] == "Course level"
    module_level_mask: pd.Series = df["Setup fee at"] == "Module level"


    # - 1.1.1. SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level
    invalid_setup_fee_at_mask_1: pd.Series = (
        (sctp_course_mask)
        & (modularised_bundle_sctp_course_mask)
        & (~module_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_1,
        column="Setup fee at",
        validation_type="Special logic",
        message='If SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level',
        extra_message='All SCTP type = Modularised bundle SCTP course -> Setup fee at: Module level',
        sheet_name="CourseIntakeFeeSetup",
    )

    # - 1.1.2. SCTP type = Non-Modularised SCTP course ->  Setup fee at: Course level or Module level -> No need to validate here, if fail, it is invalid value

    # - 1.1.3. SCTP type = SCTP as normal TPG course -> Setup fee at: Course level
    invalid_setup_fee_at_mask_3: pd.Series = (
        (sctp_course_mask)
        & (sctp_as_normal_tpg_course_mask)
        & (~course_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_3,
        column="Setup fee at",
        validation_type="Special logic",
        message="SCTP type = SCTP as normal TPG course -> Setup fee at: Course level",
        extra_message="All SCTP type = SCTP as normal TPG course -> Setup fee at: Course level",
        sheet_name="CourseIntakeFeeSetup",
    )

    # 1.2.1. SCTP type = Normal TPG course -> Setup fee at: Course level
    invalid_setup_fee_at_mask_4: pd.Series = (
        (~sctp_course_mask)
        & (normal_tpg_course_mask)
        & (~course_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_4,
        column="Setup fee at",
        validation_type="Special logic",
        message="SCTP type = Normal TPG course -> Setup fee at: Course level",
        extra_message="All SCTP type = Normal TPG course -> Setup fee at: Course level",
        sheet_name="CourseIntakeFeeSetup",
    )

    # 1.3.1. TPG applicable = Yes, SCTP course = No/Empty, SCTP type != Normal TPG course, Method to calculate = Per Pax Attendees -> Setup fee at: Course level
    invalid_setup_fee_at_mask_5: pd.Series = (
        (tpg_applicable_mask)
        & (
            df["SCTP course"].isna()
            | (~normal_tpg_course_mask & ~sctp_course_mask)
        )
        & (df["Method to calculate"] == "Per Pax Attendees")
        & (~course_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_5,
        column="Setup fee at",
        validation_type="Special logic",
        message="TPG applicable = Yes, SCTP course = No/Empty, SCTP type != Normal TPG course, Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        extra_message="All TPG applicable = Yes, SCTP course = No/Empty, SCTP type != Normal TPG course, Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        sheet_name="CourseIntakeFeeSetup",
    )

    # 2.1. TPG Applicable = No, Blended course = Yes, Method to calculate = Per Pax Attendees -> Setup fee at: Course level
    invalid_setup_fee_at_mask_6: pd.Series = (
        (~tpg_applicable_mask)
        & (blended_course_mask)
        & (df["Method to calculate"] == "Per Pax Attendees")
        & (~course_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_6,
        column="Setup fee at",
        validation_type="Special logic",
        message="TPG Applicable = No, Blended course = Yes, Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        extra_message="All TPG Applicable = No, Blended course = Yes, Method to calculate = Per Pax Attendees -> Setup fee at: Course level",
        sheet_name="CourseIntakeFeeSetup",
    )

    # 3.1. TPG Applicable = No, Blended course = No,  Method to calculate = Per Pax Attendees, Min. pax -> Setup fee at: Course level or Module level -> No need to validate here, if invalid, it will be invalid value

    # 3.2. TPG Applicable = No, Blended course = No, Method to calculate = Tier based, Lump sum, Per intake -> Setup fee at: Course level
    invalid_setup_fee_at_mask_8: pd.Series = (
        (~tpg_applicable_mask)
        & (blended_course_mask)
        & (df["Method to calculate"].isin(["Tier based", "Lump sum", "Per intake"]))
        & (~course_level_mask)
    )
    mark_result(
        df=df,
        mask=invalid_setup_fee_at_mask_8,
        column="Set up fee at",
        validation_type="Special logic",
        message="TPG Applicable = No, Blended course = No, Method to calculate = Tier based, Lump sum, Per intake -> Setup fee at: Course level",
        extra_message="All TPG Applicable = No, Blended course = No, Method to calculate = Tier based, Lump sum, Per intake -> Setup fee at: Course level",
        sheet_name="CourseIntakeFeeSetup"
    )


    return df


@logger_wrapper
def course_intake_fee_setup(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    df_check = df.copy()

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Course
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    df_course = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=[
            "CourseUniqueId",
            "TPG Applicable",
            "SCTP course",
            "CPE course",
            "Blended course",
            "Funded course",
            "SCTP type"
        ],
    )

    # Course fee setup
    df_course_fee_setup: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=[
            "CourseUniqueId",
            "Cash payment mode",
            "Subsidy payment mode"
        ]
    )
    df_course_fee_setup = df_course_fee_setup.rename(columns={"Cash payment mode": "Course cash", "Subsidy payment mode": "Course subsidy"})

    # Course intake
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
    df_intake: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntake",
        usecols=[
            "CourseUniqueId",
            "CourseIntakeId",
            "Open to"
        ]
    )

    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    df_course = df_course.merge(df_course_fee_setup, how="left", on="CourseUniqueId")
    df_intake = df_intake.merge(df_course, how="left", on="CourseUniqueId")
    df_check = df_check.merge(df_intake, how="left", on="CourseIntakeId")


    # ----------------------------------------
    # Process data
    # ----------------------------------------
    # Cash/Subsidy payment mode
    df_check["Cash payment mode"] = df_check["Cash payment mode"].map(lambda x: set(x.split("#;")) if pd.notna(x) else set())
    df_check["Course cash"] = df_check["Course cash"].map(lambda x: set(x.split("#;")) if pd.notna(x) else set())
    mismatch_cash_mask: pd.Series = df_check.apply(lambda x: not x["Cash payment mode"].issubset(x["Course cash"]), axis=1)



    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # course intake id in course intake but not in course intake fee
    intake_id_not_in_intake_fee_mask: pd.Series = ~df_intake["CourseIntakeId"].unique().isin(df_check["CourseIntakeId"].unique())
    intake_id_not_in_intake_fee_values: pd.Series = df_intake.loc[intake_id_not_in_intake_fee_mask, "CourseIntakeId"]
    if not intake_id_not_in_intake_fee_values.empty:
        logger.error(f"[CourseIntakeFeeSetup - CourseIntakeId] [Count value] Some course intakes do not have course intake fee. Amount: {intake_id_not_in_intake_fee_values.shape[0]}. Detail: {intake_id_not_in_intake_fee_values.values.tolist()}")
    else:
        logger.success("[CourseIntakeFeeSetup - CourseIntakeId] [Count value] All course intakes have course intake fee.")

    # Test point : All available cash payment mode of intake must be the same as course
    mark_result(
        df=df_check,
        mask=mismatch_cash_mask,
        column="Cash payment mode",
        validation_type="Special logic",
        message="All cash payment mode of intake must be the same as course",
        extra_message="All intake cash payment modes are in course cash payment mode",
        sheet_name="CourseIntakeFeeSetup"
    )

    # SFC just applied for is only applicable if "Course setup > Course >TPG Applicable" is "Yes"
    is_tpg_applicable_mask: pd.Series = df_check["TPG Applicable"] == "Yes"
    is_contain_sfc_mask: pd.Series = df_check["Subsidy payment mode"].astype(str).str.contains("SFC")
    invalid_sfc_mask: pd.Series = ~is_tpg_applicable_mask & is_contain_sfc_mask
    mark_result(
        df=df_check,
        mask=invalid_sfc_mask,
        column="Subsidy payment mode",
        validation_type="Special logic",
        message="SFC just applied for is only applicable if Course setup > Course > TPG Applicable is Yes",
        extra_message="No course has TPG applicable = No has SFC subsidy payment mode",
        sheet_name="CourseIntakeFeeSetup"
    )


    # Method to calculate logic
    df_check = check_method_to_calculate(df_check)

    # Set up fee logic
    df_check = check_setup_fee_logic(df_check)

    df["validation_result"] = df_check["validation_result"]
    return df