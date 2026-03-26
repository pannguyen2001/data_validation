import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data



@logger_wrapper
def course_module_fee_rate(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    df_course_fee_setup = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=[
            "CourseUniqueId",
            "Setup fee at"
        ]
    )

    # check course unique id is in course fee setup = module level but not in course module fee rate
    module_level_courses: pd.Series = df_course_fee_setup.loc[df_course_fee_setup["Setup fee at"] == "Module level", "CourseUniqueId"].unique()
    not_in_course_module_fee = set(module_level_courses) - set(df["CourseUniqueId"].unique())
    if len(not_in_course_module_fee) > 0:
        logger.error(f"[CourseUniqueId] [Special logic] Some courses in course fee setup have module level but not in course module fee rate: {not_in_course_module_fee}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All courses in course fee setup have module level are in course module fee rate")


    # check not module level but exist in course module fee rate
    course_level_courses: pd.Series = df_course_fee_setup.loc[df_course_fee_setup["Setup fee at"] != "Module level", "CourseUniqueId"].unique()
    in_course_module_fee_mask: pd.Series = (
        df["CourseUniqueId"].isin(course_level_courses)
    )
    mark_result(
        df,
        in_course_module_fee_mask,
        column="Setup fee at",
        validation_type="Special logic",
        message="Course in course fee setup = course level but exist in course module fee rate",
        extra_message="No Course in course fee setup = course level but exist in course module fee rate"
    )

    return df
