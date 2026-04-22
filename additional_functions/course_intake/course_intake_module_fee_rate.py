import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data

@logger_wrapper
def course_intake_module_fee_rate(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Pathway
    df_pathway: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "Pathway",
        usecols=[
            "CourseUniqueId",
            "Pathway ID"
        ],
    )

    # Pathway structure
    df_pathway_structure: pd.DataFrame = read_reference_data(
        course_setup_file_path,
        "PathwayStructure",
        usecols=[
            "Pathway ID",
            "Module code"
        ],
    )

    # Course intake
    df_intake: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntake",
        usecols=[
            "CourseIntakeId",
            "CourseUniqueId"
        ]
    )

    # Course intake fee
    df_intake_fee: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeFeeSetup",
        usecols=[
            "CourseIntakeId",
            "Setup fee at"
        ]
    )


    # ----------------------------------------
    # Merge data
    # ----------------------------------------
    df_pathway = df_pathway.loc[df_pathway["CourseUniqueId"].isin(df_intake["CourseUniqueId"])]
    df_pathway = df_pathway.merge(df_pathway_structure, how="left", on="Pathway ID")
    df_pathway = df_pathway.groupby("CourseUniqueId")["Module code"].apply(list).reset_index()

    df_intake = df_intake.merge(df_pathway, how="left", on="CourseUniqueId")
    df_intake = df_intake.rename(columns={"Module code": "All modules"})


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # not module level but exist in course intake module fee rate
    course_level_intake_mask: pd.Series = df_intake_fee["Setup fee at"] == "Course level"
    course_level_intake_id: pd.Series = df_intake_fee.loc[course_level_intake_mask, "CourseIntakeId"]
    invalid_intake_id_mask: pd.Series = df_check["CourseIntakeId"].isin(course_level_intake_id)
    if not invalid_intake_id_mask.empty:
        mark_result(
            df_check,
            invalid_intake_id_mask,
            column="CourseIntakeId",
            message="Intake has Setup fee at = Course level, should not exist in CourseIntakeModuleFeeSetup",
            validation_type="Special logic",
            sheet_name="CourseIntakeModuleFeeSetup",
            extra_message="No intake has Setup fee at = Course level exists in CourseIntakeModuleFeeSetup"
        )
    df["validation_result"] = df_check["validation_result"]

    #  module level but not exist in course intake module fee rate
    module_level_intake_mask: pd.Series = df_intake_fee["Setup fee at"] == "Module level"
    missing_intake_ids_maks: pd.Series = ~df_intake_fee["CourseIntakeId"].isin(df_check["CourseIntakeId"])
    missing_intake_ids: pd.Series = df_intake_fee.loc[missing_intake_ids_maks & module_level_intake_mask, "CourseIntakeId"]
    if not missing_intake_ids.empty:
        logger.error(f"[CourseIntakeModuleFeeRate - CourseIntakeId] [Special logic] Some intakes have Setup fee at = Module level but not exist in course intake module fee rate. Amount: {missing_intake_ids.shape[0]}. Detail: {missing_intake_ids.to_markdown()}")

    else:
        logger.success("[CourseIntakeModuleFeeRate - CourseIntakeId] [Special logic] All intakes have Setup fee at = Module level are in CourseIntakeModuleFeeSetup")

    # missing modules, or extra modules not in pathway
    df_check = df_check.groupby("CourseIntakeId")["Module code"].apply(list).reset_index()
    df_check = df_check.merge(df_intake, how="left", on="CourseIntakeId")
    df_check["Module code"] = df_check["Module code"].map(lambda x: set(x) if isinstance(x, list) else set())
    df_check["All modules"] = df_check["All modules"].map(lambda x: set(x) if isinstance(x, list) else set())

    is_diff_module_mask: pd.Series = (
        (df_check.apply(lambda x: not x["Module code"].issubset(x["All modules"]), axis=1))
        | (df_check.apply(lambda x: not x["All modules"].issubset(x["Module code"]), axis=1))
        )
    diff_module_values: pd.DataFrame = df_check.loc[is_diff_module_mask, ["CourseIntakeId", "Module code", "All modules"]]

    if not diff_module_values.empty:
        diff_module_values["Extra modules"] = diff_module_values.apply(lambda x: x["Module code"] - x["All modules"], axis=1)
        diff_module_values["Extra modules"] = diff_module_values["Extra modules"].map(
            lambda x: "\n".join(x) if isinstance(x, set) else "")
        diff_module_values["Missing modules"] = diff_module_values.apply(lambda x: x["All modules"] - x["Module code"], axis=1)
        diff_module_values["Missing modules"] = diff_module_values["Missing modules"].map(lambda x: "\n".join(x) if isinstance(x, set) else "")
        diff_module_values = diff_module_values.reset_index(drop=True)
        diff_module_values.index = diff_module_values.index + 1
        diff_module_values = diff_module_values.drop(columns=["Module code", "All modules"])
        logger.error(f"[CourseIntakeModuleFeeRate - CourseIntakeId] [Special logic] Some course intakes have extra module or missing module. Amount: {diff_module_values.shape[0]}. Detail:\n{diff_module_values.to_markdown()}")
    else:
        logger.success("[CourseIntakeModuleFeeRate - CourseIntakeId] [Special logic] All course intakes have correct module.")

    return df