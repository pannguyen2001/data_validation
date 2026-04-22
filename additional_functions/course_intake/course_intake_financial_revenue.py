import pandas as pd
import numpy as np
from typing import Optional, List
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data
from configs.constants import report_folder_path, datetime_today


@logger_wrapper
def check_revenue_type(df_check: pd.DataFrame):
    """
    Revenue type rule
    a. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum", then select "Revenue type" is "Revenue"
    b. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Tier based", then select "Revenue type" is "Revenue tier"
    c. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per intake", then create two entries for the same CourseIntakeId:
    - One with "Revenue type" is "Revenue per intake"
    - One with "Revenue type" is "Revenue per additional learner"
    """
    # a. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum", then select "Revenue type" is "Revenue"
    invalid_revenue_mask_1: pd.Series = (
        (df_check["Method to calculate"].isin([ "Per Pax Attendees", "Min. pax", "Lump sum"]))
        & (df_check["Revenue type"] != "Revenue")
    )
    invalid_intake_index_1: pd.Series = df_check.loc[invalid_revenue_mask_1, "index"]
    invalid_intake_index_1: pd.Series = invalid_intake_index_1.explode().unique()

    # b. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Tier based", then select "Revenue type" is "Revenue tier"
    invalid_revenue_mask_2: pd.Series = (
        (df_check["Method to calculate"] == "Tier based")
        & (df_check["Revenue type"] != "Revenue tier")
    )
    invalid_intake_index_2: pd.Series = df_check.loc[invalid_revenue_mask_2, "index"]
    invalid_intake_index_2: pd.Series = invalid_intake_index_2.explode().unique()

    # c. "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per intake", then create two entries for the same CourseIntakeId:
    # - One with "Revenue type" is "Revenue per intake"
    # - One with "Revenue type" is "Revenue per additional learner"
    invalid_revenue_mask_3: pd.Series = (
        (df_check["Method to calculate"] == "Per intake")
        & (~df_check["Revenue type"].isin(["Revenue per intake", "Revenue per additional learner"]))
    )
    invalid_intake_index_3: pd.Series = df_check.loc[invalid_revenue_mask_3, "index"]
    invalid_intake_index_3: pd.Series = invalid_intake_index_3.explode().unique()

    return (
        invalid_intake_index_1,
        invalid_intake_index_2,
        invalid_intake_index_3
    )


@logger_wrapper
def check_modules(df_check: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    # --- Missing modules ---
    df_check["Missing modules"] = df_check["All modules"] - df_check["Course/Product code/Module"]
    missing_mask = df_check["Missing modules"].map(bool)


    # --- Extra modules ---
    df_check["Extra modules"] = df_check["Course/Product code/Module"] - df_check["All modules"]
    extra_mask = df_check["Extra modules"].map(bool)

    df_final = df_check.loc[missing_mask | extra_mask, ["CourseIntakeId", "Missing modules", "Extra modules"]]
    if not df_final.empty:
        df_final["Missing modules"] = df_final["Missing modules"].map(lambda x: "\n".join(set(x)) if pd.notna(x) else "")
        df_final["Extra modules"] = df_final["Extra modules"].map(lambda x: "\n".join(set(x)) if pd.notna(x) else "")
        df_final = df_final.reset_index(drop=True)
        df_final.index = df_final.index + 1
        logger.error(f"[CourseIntakeFinancialRevenue - Course/Product code/Module] [Special logic] Missing and extra modules. Amount: {len(df_final)}. Details:\n{df_final.to_markdown()}")
    else:
        logger.success("[CourseIntakeFinancialRevenue - Course/Product code/Module] [Special logic] No missing and extra modules")


@logger_wrapper
def check_sum_of_amount(df_check: pd.DataFrame):
    # Flat rate
    rate_cols: List = [
        "Amount",
        "Sum of flat rate",
        "Sum of charge rate",
        "Sum of package rate of",
        "Sum of module fee rate",
        "Sum of tier fee rate"
    ]
    df_check[rate_cols] = df_check[rate_cols].fillna(0).astype(float).round(4)
    df_check = df_check.reset_index()

    invalid_flat_rate_mask: pd.Series = (
        (df_check["Sum of flat rate"] > 0)
        & (df_check["Revenue type"] == "Revenue")
        & (df_check["Amount"] != df_check["Sum of flat rate"])
        )
    invalid_flat_rate_index: pd.Series = df_check.loc[invalid_flat_rate_mask, "index"]
    invalid_flat_rate_index: pd.Series = invalid_flat_rate_index.explode().unique()

    invalid_module_rate_mask: pd.Series = (
        (df_check["Sum of module fee rate"] > 0)
        & (df_check["Revenue type"] == "Revenue")
         & (df_check["Amount"] != df_check["Sum of module fee rate"])
    )
    invalid_module_rate_index: pd.Series = df_check.loc[invalid_module_rate_mask, "index"]
    invalid_module_rate_index: pd.Series = invalid_module_rate_index.explode().unique()

    invalid_package_rate_of_mask: pd.Series = (
        (df_check["Sum of package rate of"] > 0)
        & (df_check["Revenue type"] == "Revenue per intake")
        & (df_check["Amount"] != df_check["Sum of package rate of"])
    )
    invalid_package_rate_of_index: pd.Series = df_check.loc[invalid_package_rate_of_mask, "index"]
    invalid_package_rate_of_index: pd.Series = invalid_package_rate_of_index.explode().unique()

    invalid_charge_rate_mask: pd.Series = (
        (df_check["Sum of charge rate"] > 0)
        & (df_check["Revenue type"] == "Revenue per additional learner")
        & (df_check["Amount"] != df_check["Sum of charge rate"])
    )
    invalid_charge_rate_index: pd.Series = df_check.loc[invalid_charge_rate_mask, "index"]
    invalid_charge_rate_index: pd.Series = invalid_charge_rate_index.explode().unique()

    invalid_tier_fee_rate_mask: pd.Series = (
        (df_check["Sum of tier fee rate"] > 0)
        & (df_check["Revenue type"] == "Revenue tier")
        & (df_check["Amount"] != df_check["Sum of tier fee rate"])
    )
    invalid_tier_fee_rate_index: pd.Series = df_check.loc[invalid_tier_fee_rate_mask, "index"]
    invalid_tier_fee_rate_index = invalid_tier_fee_rate_index.explode().unique()

    return (
        invalid_flat_rate_index,
        invalid_charge_rate_index,
        invalid_module_rate_index,
        invalid_package_rate_of_index,
        invalid_tier_fee_rate_index
    )


@logger_wrapper
def course_intake_financial_revenue(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    rate_cols: List = [
            "Flat rate per pax (without GST)",
            "Package rate (without GST)",
            "Package rate of (without GST)",
            "Charge rate of (without GST) per additional learner",
        ]


    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # CourseFinancialSetupRevenue
    df_course_financial_setup_revenue: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "CourseFinancialSetupRevenue",
        usecols=["CourseUniqueId"]
    )

    # CourseIntake
    df_intake: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntake",
        usecols=["CourseIntakeId", "CourseUniqueId", "Status"],
    )

    # CourseIntakeFeeStup
    df_intake_fee_setup: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeFeeSetup",
        usecols=["CourseIntakeId", "Method to calculate", "Setup fee at"] + rate_cols,
    )

    # CourseIntakeModuleFeeRate
    df_intake_module_fee_rate: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeModuleFeeRate"
    )

    # CourseIntakeTierRateFee
    df_intake_tier_rate_fee: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeTierRateFee"
    )

    # Course
    df_course: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=["CourseUniqueId"],
    )

    # ElearningCourseConfig
    df_elearning_course_config: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "ELearningCourseConfiguration",
        usecols=["CourseUniqueId", "Selected courses"]
    )

    # Pathway
    df_pathway: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "Pathway",
        usecols=["CourseUniqueId", "Pathway ID"]
    )

    # PathwayStructure
    df_pathway_structure: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "PathwayStructure",
        usecols=["Pathway ID", "Module code"]
    )

    # ----------------------------------------
    # Process data
    # ----------------------------------------
    financial_setup_revenue_course_ids: set = set(df_course_financial_setup_revenue["CourseUniqueId"])

    df_intake_fee_setup[rate_cols] = df_intake_fee_setup[rate_cols].astype(float).fillna(0)
    df_intake_fee_setup["Sum of charge rate"] = df_intake_fee_setup["Charge rate of (without GST) per additional learner"].fillna(0)
    df_intake_fee_setup["Sum of package rate of"] = df_intake_fee_setup["Package rate of (without GST)"].fillna(0)
    df_intake_fee_setup.loc[(
        (df_intake_fee_setup["Setup fee at"] != "Course level")
        & (df_intake_fee_setup["Method to calculate"] == "Per Pax Attendees")
    ), rate_cols] = 0
    df_intake_fee_setup["Sum of flat rate"] = df_intake_fee_setup[rate_cols].sum(axis=1)
    df_intake_fee_setup = df_intake_fee_setup.drop(columns=rate_cols)

    df_intake_module_fee_rate["Module fee per pax (without GST)"] = df_intake_module_fee_rate["Module fee per pax (without GST)"].astype(float).fillna(0)
    df_intake_module_fee_rate = df_intake_module_fee_rate.rename(columns={"Module fee per pax (without GST)": "Sum of module fee rate"})
    df_intake_module_fee_rate = df_intake_module_fee_rate.groupby("CourseIntakeId").agg({"Sum of module fee rate": "sum"}).reset_index()

    df_intake_tier_rate_fee["Flat rate per pax (without GST)"] = df_intake_tier_rate_fee["Flat rate per pax (without GST)"].astype(float).fillna(0)
    df_intake_tier_rate_fee = df_intake_tier_rate_fee.rename(columns={"Flat rate per pax (without GST)": "Sum of tier fee rate"})
    df_intake_tier_rate_fee = df_intake_tier_rate_fee.groupby("CourseIntakeId").agg({"Sum of tier fee rate": "sum"}).reset_index()

    df_elearning_course_config = df_elearning_course_config.groupby("CourseUniqueId").agg({"Selected courses": list}).reset_index()

    df_pathway_structure = df_pathway_structure.groupby("Pathway ID").agg({"Module code": list}).reset_index()

    df_check["Amount"] = df_check["Amount"].fillna(0).astype(float).round(4)
    df_check = df_check.reset_index().groupby(["CourseIntakeId", "Revenue type"]).agg({"Course/Product code/Module": list, "Amount": "sum", "index": list}).reset_index()
    df_check["Course/Product code/Module"] = df_check["Course/Product code/Module"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())


    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    # Course vs Pathway
    df_course = df_course.merge(df_pathway, how="left", on="CourseUniqueId")

    # Course-Pathway vs PathwayStructure
    df_course = df_course.merge(df_pathway_structure, how="left", on="Pathway ID")

    # Course-PathwayStructure vs ElearningCourseConfig
    df_course = df_course.merge(df_elearning_course_config, how="left", on="CourseUniqueId")
    df_course["Module code"] = df_course["Module code"].map(lambda x: [] if not isinstance(x, list) else x)
    df_course["Selected courses"] = df_course["Selected courses"].map(lambda x: [] if not isinstance(x, list) else x)
    df_course["All modules"] = df_course["Module code"] + df_course["Selected courses"]
    df_course = df_course.explode("All modules", ignore_index=True)
    df_course = df_course.groupby("CourseUniqueId").agg({"All modules": list}).reset_index()
    df_course["All modules"] = df_course["All modules"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())

    # Course vs Intake
    df_intake = df_intake.merge(df_course, how="left", on="CourseUniqueId")

    # Intake vs IntakeFeeSetup
    df_intake_fee_setup = df_intake_fee_setup.merge(df_intake, how="left", on="CourseIntakeId")

    # IntakeFeeSetup vs IntakeModuleFeeRate
    df_intake_fee_setup = df_intake_fee_setup.merge(df_intake_module_fee_rate, how="left", on="CourseIntakeId")

    # IntakeFeeSetup vs IntakeTierFeeRate
    df_intake_fee_setup = df_intake_fee_setup.merge(df_intake_tier_rate_fee, how="left", on="CourseIntakeId")

    # IntakeFeeSetup vs IntakeFinancialRevenue
    df_check = df_check.merge(df_intake_fee_setup, how="left", on="CourseIntakeId")


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # CourseIntake that has course unique id not in CourseFinancialSetupRevenue
    missing_financial_setup_revenue_ids: set = set(df_intake.loc[~df_intake["CourseUniqueId"].isin(financial_setup_revenue_course_ids), "CourseIntakeId"])
    if len(missing_financial_setup_revenue_ids) > 0:
        logger.error(f"[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] CourseIntakeId in CourseIntakeFinancialRevenue but CourseUniqueId not found in CourseFinancialSetupRevenue. Amount: {len(missing_financial_setup_revenue_ids)}. Details: {missing_financial_setup_revenue_ids}")
    else:
        logger.success("[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] CourseIntakeId in CourseIntakeFinancialRevenue and CourseUniqueId found in CourseFinancialSetupRevenue.")

    # CourseIntakeId in CourseIntake but not in CourseIntakeFinancialRevenue
    need_check_status_list: List = ["Ready for application", "Application closed", "Open for application", "Commenced", "Cancelled"]
    missing_intake_ids: set = set(df_intake.loc[df_intake["Status"].isin(need_check_status_list), "CourseIntakeId"]) - set(df_check["CourseIntakeId"])
    if len(missing_intake_ids) > 0:
        logger.error(f"[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] CourseIntakeId in CourseIntake but not in CourseIntakeFinancialRevenue. Amount: {len(missing_intake_ids)}. Details: {missing_intake_ids}")
    else:
        logger.info("[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] All CourseIntakeId in CourseIntake are in CourseIntakeFinancialRevenue.")

    # CourseIntakeId in CourseIntakeFeeSetup but not in CourseIntakeFinancialRevenue
    missing_intake_fee_setup_ids: set = set(df_intake_fee_setup.loc[df_intake_fee_setup["Status"].isin(need_check_status_list), "CourseIntakeId"]) - set(df_check["CourseIntakeId"])
    if len(missing_intake_fee_setup_ids) > 0:
        logger.error(f"[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] CourseIntakeId in CourseIntakeFeeSetup but not in CourseIntakeFinancialRevenue. Amount: {len(missing_intake_fee_setup_ids)}. Details: {missing_intake_fee_setup_ids}")
    else:
        logger.info("[CourseIntakeFinancialRevenue - CourseIntakeId] [Special logic] All CourseIntakeId in CourseIntakeFeeSetup are in CourseIntakeFinancialRevenue.")

    # Revenue type
    (
        invalid_intake_index_1,
        invalid_intake_index_2,
        invalid_intake_index_3
    ) = check_revenue_type(df_check)

    invalid_intake_index_mask_1: pd.Series = pd.Series(df.index.isin(invalid_intake_index_1))
    mark_result(
        df=df,
        mask=invalid_intake_index_mask_1,
        column="Revenue type",
        validation_type="Special logic",
        message='"Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum", "Revenue type" is "Revenue"',
        extra_message='All courses have "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum" and "Revenue type" is "Revenue"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_intake_index_mask_2: pd.Series = pd.Series(df.index.isin(invalid_intake_index_2))
    mark_result(
        df=df,
        mask=invalid_intake_index_mask_2,
        column="Revenue type",
        validation_type="Special logic",
        message='"Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Tier based", "Revenue type" is "Revenue tier"',
        extra_message='All courses have "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Tier based" and "Revenue type" is "Revenue tier"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_intake_index_mask_3: pd.Series = pd.Series(df.index.isin(invalid_intake_index_3))
    mark_result(
        df=df,
        mask=invalid_intake_index_mask_3,
        column="Revenue type",
        validation_type="Special logic",
        message='"Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per intake", "Revenue type" is "Revenue per intake" or "Revenue per additional learner"',
        extra_message='All courses have "Course IntakeSetupAndOthers > CourseIntakeFeeSetup > Method to calculate" is "Per intake" and "Revenue type" is "Revenue per intake" or "Revenue per additional learner"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    # Missing modules or Extra modules
    check_modules(df_check)

    # Sum of Amount
    (
        invalid_flat_rate_index,
        invalid_charge_rate_index,
        invalid_module_rate_index,
        invalid_package_rate_of_index,
        invalid_tier_fee_rate_index
    ) = check_sum_of_amount(df_check)

    invalid_flat_rate_index_mask: pd.Series = pd.Series(df.index.isin(invalid_flat_rate_index))
    mark_result(
        df=df,
        mask=invalid_flat_rate_index_mask,
        column="Amount",
        validation_type="Special logic",
        message="Sum of flat rate is not equal to Amount",
        extra_message='All courses have "Sum of flat rate" is equal to "Amount"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_charge_rate_index_mask: pd.Series = pd.Series(df.index.isin(invalid_charge_rate_index))
    mark_result(
        df=df,
        mask=invalid_charge_rate_index_mask,
        column="Amount",
        validation_type="Special logic",
        message="Sum of charge rate is not equal to Amount",
        extra_message='All courses have "Sum of charge rate" is equal to "Amount"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_package_rate_of_index_mask: pd.Series = pd.Series(df.index.isin(invalid_package_rate_of_index))
    mark_result(
        df=df,
        mask=invalid_package_rate_of_index_mask,
        column="Amount",
        validation_type="Special logic",
        message="Sum of package rate of is not equal to Amount",
        extra_message='All courses have "Sum of package rate of" is equal to "Amount"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_module_index_mask: pd.Series = pd.Series(df.index.isin(invalid_module_rate_index))
    mark_result(
        df=df,
        mask=invalid_module_index_mask,
        column="Amount",
        validation_type="Special logic",
        message="Sum of module fee rate is not equal to Amount",
        extra_message='All courses have "Sum of module fee rate" is equal to "Amount"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    invalid_tier_fee_rate_index_mask: pd.Series = pd.Series(df.index.isin(invalid_tier_fee_rate_index))
    mark_result(
        df=df,
        mask=invalid_tier_fee_rate_index_mask,
        column="Amount",
        validation_type="Special logic",
        message="Sum of tier fee rate is not equal to Amount",
        extra_message='All courses have "Sum of tier fee rate" is equal to "Amount"',
        sheet_name="CourseIntakeFinancialRevenue"
    )

    return df


# @logger_wrapper
# def check_modules(df_check: pd.DataFrame):
#     df_check["All modules"] = df_check["All modules"].apply(lambda x: set(x) if isinstance(x, set) or isinstance(x, list) else set())
#     df_check["Course/Product code/Module"] = df_check["Course/Product code/Module"].apply(lambda x: set(x) if isinstance(x, set) or isinstance(x, list) else set())
#     # Missing modules
#     missing_modules_mask: pd.Series = df_check.apply(lambda x: not x["All modules"].issubset(x["Course/Product code/Module"]), axis=1)
#     missing_modules_ids: pd.Series = df_check.loc[missing_modules_mask, "index"]
#     missing_modules_ids = missing_modules_ids.explode("index").unique()
#     if missing_modules_mask.any():
#         df_missing_modules: pd.DataFrame = df_check.loc[missing_modules_mask, ["CourseIntakeId", "All modules", "Course/Product code/Module"]]
#         df_missing_modules["Missing modules"] = df_missing_modules.apply(lambda x: x["All modules"] - x["Course/Product code/Module"], axis=1)
#         logger.error(f"[CourseIntakeFinancialRevenue - Course/Product code/Module] [Special logic] Some courses have missing modules. Amount: {len(df_missing_modules)}. Details: {df_missing_modules[['CourseIntakeId', 'Missing modules']].to_dict(orient='records')}")
#     else:
#         logger.success("[CourseIntakeFinancialRevenue - Course/Product code/Module] [Special logic] No missing course/ product code/ module.")

#     # Extra modules
#     extra_modules_mask: pd.Series = df_check.apply(lambda x: not x["Course/Product code/Module"].issubset(x["All modules"]), axis=1)
#     extra_modules_ids: pd.Series = df_check.loc[extra_modules_mask, "index"]
#     extra_modules_ids = extra_modules_ids.explode("index").unique()
#     if extra_modules_mask.any():
#         df_extra_modules: pd.DataFrame = df_check.loc[extra_modules_mask, ["CourseIntakeId", "All modules", "Course/Product code/Module"]]
#         df_extra_modules["Extra modules"] = df_extra_modules.apply(lambda x: x["Course/Product code/Module"] - x["All modules"], axis=1)
#         logger.error(f"[CourseIntakeFinancialRevenue - Course/Product code/Module] [Special logic] Some courses have extra modules. Amount: {len(df_extra_modules)}. Details: {df_extra_modules[['CourseIntakeId', 'Extra modules']].to_dict(orient='records')}")

#     return missing_modules_ids, extra_modules_ids


# missing_modules_ids, extra_modules_ids = check_modules(df_check)
    # missing_modules_index_mask: pd.Series = pd.Series(df.index.isin(missing_modules_ids))
    # mark_result(
    #     df=df,
    #     mask=missing_modules_index_mask,
    #     column="Course/Product code/Module",
    #     validation_type="Special logic",
    #     message='Intake has missing modules',
    #     extra_message='All intakes do not have missing modules',
    #     sheet_name="CourseIntakeFinancialRevenue"
    # )
    # extra_modules_index_mask: pd.Series = pd.Series(df.index.isin(extra_modules_ids))
    # mark_result(
    #     df=df,
    #     mask=extra_modules_index_mask,
    #     column="Course/Product code/Module",
    #     validation_type="Special logic",
    #     message='Intake has extra modules',
    #     extra_message='All intakes do not have extra modules',
    #     sheet_name="CourseIntakeFinancialRevenue"
    # )