import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.read_reference_data import read_reference_data


@logger_wrapper
def course_module_fee_rate(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
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
            "Blended course",
            "E-learning course configuration?"
        ],
    )

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    is_blended_course_mask: pd.Series = df_course["Blended course"] == "Yes"
    is_e_learning_config_mask: pd.Series = df_course["E-learning course configuration?"] == "Yes"

    # course has: "Blended course" is "Yes" and "E-learning course configuration?" is "Yes" but not in this sheet
    blended_course_ids: pd.Series = df_course.loc[is_blended_course_mask & is_e_learning_config_mask, "CourseUniqueId"]
    missing_e_learning_config_ids: pd.Series = blended_course_ids[~blended_course_ids.isin(df["CourseUniqueId"].unique())]
    if not missing_e_learning_config_ids.empty:
        logger.error(f"[CourseUniqueId] [Special logic] Some blended courses have e-learning configuration = Yes but have no data in ELearningCourseConfiguration. Amount: {missing_e_learning_config_ids.shape[0]}. Detail: {missing_e_learning_config_ids.values.tolist()}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All blended courses have e-learning configuration = Yes have data in ELearningCourseConfiguration.")

    return df