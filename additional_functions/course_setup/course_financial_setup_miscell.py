import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.read_reference_data import read_reference_data


@logger_wrapper
def course_financial_setup_miscell(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:

    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    df_course_fee_setup_misc_item = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetupMiscellaneousItem",
        usecols=["CourseUniqueId","Miscellaneous item name", "Miscellaneous type"],
    )

    # Check id + misc in CourseFeeSetupMiscellaneousItem but not in CourseFinancialSetupMiscell
    id_and_misc_financial = set(zip(df[["CourseUniqueId","Miscellaneous item name"]]))
    id_and_misc_items = set(zip(df_course_fee_setup_misc_item[["CourseUniqueId","Miscellaneous item name"]]))

    lack_id_and_misc = id_and_misc_items - id_and_misc_financial
    if len(lack_id_and_misc) > 0:
        logger.error(f"[CourseFinancialSetupMiscell - [CourseUniqueId - Miscellaneous item name]] [Special logic] Data in CourseFeeSetupMiscellaneousItem but not in CourseFinancialSetupMiscell. Amount: {len(lack_id_and_misc)}. Details: {lack_id_and_misc}.")
    else:
        logger.success("[CourseFinancialSetupMiscell - [CourseUniqueId - Miscellaneous item name]] [Special logic] All Data in CourseFeeSetupMiscellaneousItem are in CourseFinancialSetupMiscell.")

    # Check id + misc in CourseFinancialSetupMiscell but not in CourseFeeSetupMiscellaneousItem
    extra_id_and_misc = set(id_and_misc_financial) - set(id_and_misc_items)
    if len(extra_id_and_misc) > 0:
        logger.success(f"[CourseUniqueId - Miscellaneous item name] [Special logic] CourseFinancialSetupMiscell but not in CourseFeeSetupMiscellaneousItem. Amount: {len(extra_id_and_misc)}. Details: {extra_id_and_misc}.")
    else:
        logger.info("[CourseUniqueId - Miscellaneous item name] [Special logic] No CourseFinancialSetupMiscell but not in CourseFeeSetupMiscellaneousItem.")

    return df
