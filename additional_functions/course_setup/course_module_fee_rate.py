import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data
from configs.constants import additional_report_folder_path


@logger_wrapper
def check_missing_modules(df: pd.DataFrame, df_pathway: pd.DataFrame):
    # ---------- Missing modules check ----------
    df_check: pd.DataFrame = df.copy().reset_index()
    df_check["Module code"] = df_check["Module code"].fillna("").astype(str)
    df_check = (
        df_check.groupby("CourseUniqueId")
        .agg({"Module code": list, "index": list})
        .reset_index()
    )

    df_check = df_check.merge(df_pathway, how="left", on="CourseUniqueId")
    df_check["Module code"] = df_check["Module code"].map(
        lambda x: set(x) if isinstance(x, (list, set)) else set()
    )
    df_check["Module code list"] = df_check["Module code list"].map(
        lambda x: x if isinstance(x, (list, set)) else set()
    )

    # All missing modules will record in log or export to excel file if amount > 10
    df_check["Missing modules"] = df_check.apply(
        lambda x: x["Module code list"] - x["Module code"], axis=1
    )
    is_missing_module_mask: pd.Series = df_check["Missing modules"].map(
        lambda x: len(x) > 0
    )
    df_result = df_check.loc[
        is_missing_module_mask,
        ["CourseUniqueId", "Missing modules"],
    ]
    if not df_result.empty:
        df_result["Missing modules"] = df_result["Missing modules"].map(
            lambda x: ", ".join(x) if isinstance(x, set) else ""
        )
        df_result.index = df_result.index + 1
        logger.error(
            f"[CourseModuleFeeRate - Module code] [Special logic] Extra or missing modules. Amount: {df_result.shape[0]}."
        )
        if df_result.shape[0] <= 10:
            logger.error(
                f"[CourseModuleFeeRate - Module code] [Special logic] Detail:\n{df_result.to_markdown()}"
            )
        else:
            detail_report_file_path: str = f"{additional_report_folder_path}/CourseModuleFeeRate missing or extra modules report.xlsx"
            df_result.to_excel(detail_report_file_path, sheet_name="CourseModuleFeeRate", index=False)
            logger.error(
                f"[CourseModuleFeeRate - Module code] [Special logic] More than 10 error data. Detail error in: {detail_report_file_path}"
            )
    else:
        logger.success(
            "[CourseModuleFeeRate - Module code] [Special logic] All modules in course module fee rate are in pathway structure"
        )


@logger_wrapper
def check_extra_module(df: pd.DataFrame, df_pathway: pd.DataFrame):
    # ---------- Extra modules check ----------
    # Extra modules to excel report
    df_check_2: pd.DataFrame = df.copy()
    df_check_2 = df_check_2.merge(df_pathway, how="left", on="CourseUniqueId")
    extra_modules_mask: pd.Series = (
        (df_check_2["Module code"].notna())
        & (df_check_2["Module code list"].notna())
        & (df_check_2.apply(lambda x: x["Module code"] not in x["Module code list"], axis=1))
    )
    mark_result(
        df=df,
        mask=extra_modules_mask,
        column="Module code",
        validation_type="Special logic",
        message="Module code in CourseModuleFeeRate is not in pathway structure",
        sheet_name="CourseModuleFeeRate",
    )
    return df


@logger_wrapper
def course_module_fee_rate(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")


    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # CourseFeeSetup
    df_course_fee_setup = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=["CourseUniqueId", "Setup fee at"],
    )

    # Pathway
    df_pathway: pd.DataFrame = read_reference_data(
        course_setup_file_path, "Pathway", usecols=["CourseUniqueId", "Pathway ID"]
    )

    # Pathway structure
    df_pathway_structure: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "PathwayStructure",
        usecols=["Pathway ID", "Module code"],
    )


    # ----------------------------------------
    # Process data
    # ----------------------------------------
    # Get module level courses and course level course
    is_module_level_mask: pd.Series = df_course_fee_setup["Setup fee at"] == "Module level"
    module_level_courses: pd.Series = df_course_fee_setup.loc[is_module_level_mask, "CourseUniqueId"
    ].unique()
    course_level_courses: pd.Series = df_course_fee_setup.loc[
        ~is_module_level_mask, "CourseUniqueId"
    ].unique()

    # Merge Pathway vs PathwayStructure
    df_pathway = df_pathway.merge(
        df_pathway_structure, on="Pathway ID", how="left")
    df_pathway["Module code"] = df_pathway["Module code"].fillna(
        "").astype(str)
    df_pathway = (
        df_pathway.groupby("CourseUniqueId").agg(
            {"Module code": list}).reset_index()
    )
    df_pathway["Module code"] = df_pathway["Module code"].map(
        lambda x: set(x) if isinstance(x, list) else set()
    )
    df_pathway = df_pathway.rename(columns={"Module code": "Module code list"})


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # check course unique id is in course fee setup = module level but not in course module fee rate
    not_in_course_module_fee = set(module_level_courses) - set(
        df["CourseUniqueId"].unique()
    )
    if len(not_in_course_module_fee) > 0:
        logger.error(
            f"[CourseModuleFeeRate - CourseUniqueId] [Special logic] Some courses in course fee setup have module level but not in course module fee rate. Amount: {len(not_in_course_module_fee)}. Details: {not_in_course_module_fee}"
        )
    else:
        logger.success(
            "[CourseModuleFeeRate - CourseUniqueId] [Special logic] All courses in course fee setup have module level are in course module fee rate"
        )

    # check not module level but exist in course module fee rate
    in_course_module_fee_mask: pd.Series = df["CourseUniqueId"].isin(
        course_level_courses
    )
    mark_result(
        df,
        in_course_module_fee_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="Course in course fee setup = course level but exist in course module fee rate",
        extra_message="No Course in course fee setup = course level but exist in course module fee rate",
        sheet_name="CourseModuleFeeRate",
    )

    # Check module is in correct pathway structure
    # Check missing modules
    check_missing_modules(df, df_pathway)
    # Check extra modules
    df = check_extra_module(df, df_pathway)

    return df
