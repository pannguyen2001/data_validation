import pandas as pd
from typing import Optional
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_misc_item(df: pd.DataFrame, df_misc_item: pd.DataFrame) -> pd.DataFrame:
    """
    4. Only miscellaneous item that are setup as "Finance Setup > MiscellaneousItem > Currency" must have same "Course Setup > CourseFeeSetup > Currency" will be available for selection
    """
    incorrect_currency_mask: pd.Series = (
        df["Miscellaneous item name"].isin(df_misc_item["Miscellaneous item name"])
    ) & ~(df["Currency"].isin(df_misc_item["Currency"].unique()))
    mark_result(
        df,
        incorrect_currency_mask,
        column="Currency",
        validation_type="Special logic",
        message='Currency of misc item of course not the same as "Finance Setup > MiscellaneousItem > Currency" must have same "Course Setup > CourseFeeSetup > Currency"',
        extra_message='All misc items of course have the same currency as "Finance Setup > MiscellaneousItem > Currency" must have same "Course Setup > CourseFeeSetup > Currency"',
    )

    return df


@logger_wrapper
def course_fee_setup_miscellaneous_item(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")
    df_misc_item = read_reference_data(
        finance_setup_file_path,
        "MiscellaneousItem",
        usecols=["Miscellaneous item name", "Currency"],
    )

    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    df_course = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=["CourseUniqueId","Currency"],
    )

    df_check = df.copy()
    df_course = df_course.loc[
        df_course["CourseUniqueId"].isin(df_check["CourseUniqueId"])
    ]
    df_check = df_check.merge(df_course, on="CourseUniqueId", how="left")
    df_check = check_misc_item(df_check, df_misc_item)
    df["validation_result"] = df_check["validation_result"]
    return df
