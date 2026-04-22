import pandas as pd
from typing import Optional
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_discount_name(df: pd.DataFrame, df_course_fee_setup_discount: pd.DataFrame):
    df_course_fee_setup_discount = df_course_fee_setup_discount.groupby(["CourseUniqueId"]).agg({"Discount name": list}).reset_index()
    df_course_fee_setup_discount["Discount name"] = df_course_fee_setup_discount["Discount name"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())
    df_course_fee_setup_discount = df_course_fee_setup_discount.rename(columns={"Discount name": "All discounts"})

    df_check_discount = df.reset_index().groupby("CourseUniqueId").agg({"Discount name": list, "index": list}).reset_index()
    df_check_discount["Discount name"] = df_check_discount["Discount name"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())

    df_check_discount = df_check_discount.merge(df_course_fee_setup_discount, how="left", on="CourseUniqueId")
    df_check_discount["All discounts"] = df_check_discount["All discounts"].map(lambda x: x if isinstance(x, set) else set())
    df_check_discount["Discount name"] = df_check_discount["Discount name"].map(lambda x: x if isinstance(x, set) else set())

    df_check_discount["Missing discounts"] = df_check_discount.apply(lambda x: x["All discounts"] - x["Discount name"], axis=1)
    missing_discounts_mask: pd.Series = df_check_discount["Missing discounts"].map(lambda x: len(x) > 0)

    df_check_discount["Extra discounts"] = df_check_discount.apply(lambda x: x["Discount name"] - x["All discounts"], axis=1)
    extra_discounts_mask: pd.Series = df_check_discount["Extra discounts"].map(lambda x: len(x) > 0)

    df_result = df_check_discount.loc[missing_discounts_mask | extra_discounts_mask, ['CourseUniqueId', "Missing discounts", "Extra discounts"]]
    if not df_result.empty:
        df_result["Missing discounts"] = df_result["Missing discounts"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result["Extra discounts"] = df_result["Extra discounts"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result = df_result.reset_index(drop=True)
        df_result.index = df_result.index + 1
        logger.error(f"[CourseFinancialSetupDiscount - [CourseUniqueId - Discount name]] [Special logic] Some courses have missing/extra Discount names in CourseFinancialSetupDiscount that in/not in CourseFeeSetupDiscount. Amount: {df_result.shape[0]}. Details: {df_result.to_markdown()}")
    else:
        logger.success("[CourseFinancialSetupDiscount - [CourseUniqueId - Discount name]] [Special logic] All courses have Discount name in CourseFinancialSetupDiscount are in CourseFeeSetupDiscount.")


@logger_wrapper
def check_modules(df: pd.DataFrame, df_course_financial_revenue: pd.DataFrame):
    df_course_financial_revenue = df_course_financial_revenue.groupby(["CourseUniqueId"]).agg({"Course/Product code/Module": list}).reset_index()
    df_course_financial_revenue["Course/Product code/Module"] = df_course_financial_revenue["Course/Product code/Module"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())
    df_course_financial_revenue = df_course_financial_revenue.rename(columns={"Course/Product code/Module": "All modules"})

    df_check_module = df.reset_index().groupby("CourseUniqueId").agg({"Course/Product code/Module": list, "index": list}).reset_index()
    df_check_module["Course/Product code/Module"] = df_check_module["Course/Product code/Module"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())

    df_check_module = df_check_module.merge(df_course_financial_revenue, how="left", on="CourseUniqueId")
    df_check_module["All modules"] = df_check_module["All modules"].map(lambda x: x if isinstance(x, set) else set())
    df_check_module["Course/Product code"] = df_check_module["Course/Product code/Module"].map(lambda x: x if isinstance(x, set) else set())

    df_check_module["Missing modules"] = df_check_module.apply(lambda x: x["All modules"] - x["Course/Product code/Module"], axis=1)
    missing_modules_mask: pd.Series = df_check_module["Missing modules"].map(lambda x: len(x) > 0)

    df_check_module["Extra modules"] = df_check_module.apply(lambda x: x["Course/Product code/Module"] - x["All modules"], axis=1)
    extra_modules_mask: pd.Series = df_check_module["Extra modules"].map(lambda x: len(x) > 0)

    df_result = df_check_module.loc[missing_modules_mask | extra_modules_mask, ['CourseUniqueId', "Missing modules", "Extra modules"]].reset_index(drop=True)
    if not df_result.empty:
        df_result["Missing modules"] = df_result["Missing modules"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result["Extra modules"] = df_result["Extra modules"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result = df_result.reset_index(drop=True)
        df_result.index = df_result.index + 1
        logger.error(f"[CourseFinancialSetupDiscount - [CourseUniqueId - Course/Product code/Module]] [Special logic] Some courses have missing/extra Course/Product code/Module in CourseFinancialSetupDiscount that in/not in CourseFinancialSetupRevenue. Amount: {df_result.shape[0]}. Details:\n{df_result.to_markdown()}")
    else:
        logger.success("[CourseFinancialSetupDiscount - [CourseUniqueId - Course/Product code/Module]] [Special logic] All courses have Course/Product code/Module in CourseFinancialSetupDiscount are in CourseFinancialSetupRevenue.")


@logger_wrapper
def check_financial_discount_and_fee_setup_discount(df, df_course_fee_setup_discount) -> None:
    id_course_discount_fee_setup: set = set(zip(df_course_fee_setup_discount[["CourseUniqueId", "Discount name"]]))
    id_course_discount_financial_setup: set = set(zip(df[["CourseUniqueId", "Discount name"]]))
    # check id + dis CourseFeeSetupDiscount but not in CourseFinancialSetupDiscount
    lack_of_financial = id_course_discount_fee_setup - id_course_discount_financial_setup
    if len(lack_of_financial) > 0:
        logger.error(f"[CourseUniqueId] [Special logic] Some courses have Discount name in CourseFeeSetupDiscount but not in CourseFinancialSetupDiscount. Amount: {len(lack_of_financial)}. Details: {lack_of_financial}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All courses  have Discount name in CourseFeeSetupDiscount are in CourseFinancialSetupDiscount")
    # check id + dis in CourseFinancialSetupDiscount but not in CourseFeeSetupDiscount
    extra_financial = id_course_discount_financial_setup - id_course_discount_fee_setup
    if len(extra_financial) > 0:
        logger.error(f"[CourseUniqueId] [Special logic] Some courses  have Discount name in CourseFinancialSetupDiscount but not in CourseFeeSetupDiscount. Amount: {len(extra_financial)}. Details: {extra_financial}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All courses  have Discount name in CourseFinancialSetupDiscount are in CourseFeeSetupDiscount")

@logger_wrapper
def check_empty_amount_and_percentage(df_check) -> pd.DataFrame:
    # Amount: Discount rate type = Flat -> not have value: error
    empty_amount_mask: pd.Series = (
        (df_check["Discount rate type"] == "Flat")
        & (df_check["Amount"].isna())
    )
    mark_result(
        df=df_check,
        mask=empty_amount_mask,
        column="Discount name - Amount",
        validation_type="Special logic",
        message="Discount rate type = Flat but not have Amount value",
        extra_message="All discount rate type = Flat have Amount value",
        sheet_name="CourseFinancialSetupDiscount"
    )

    # Percentage of fee: Discount rate type = Percentage -> not have value: error
    empty_amount_mask: pd.Series = (
        (df_check["Discount rate type"] == "Percentage")
        & (df_check["Percentage of fee"].isna())
    )
    mark_result(
        df=df_check,
        mask=empty_amount_mask,
        column="Discount name - Percentage of fee",
        validation_type="Special logic",
        message="Discount rate type = Percentage but not have Percentage of fee value",
        extra_message="All discount rate type = Percentage have Percentage of fee value",
        sheet_name="CourseFinancialSetupDiscount"
    )
    return df_check

@logger_wrapper
def check_finacial_discount_vs_discount_setup(df_check: pd.DataFrame) -> pd.DataFrame:
    df_amount: pd.DataFrame = df_check.copy().reset_index()
    df_amount["Amount"] = df_amount["Amount"].astype(float).fillna(0)
    df_amount["Percentage of fee"] = df_amount["Percentage of fee"].astype(float).fillna(0)
    df_amount["Rate Flat"] = df_amount["Rate Flat"].astype(float)
    df_amount["Rate Percentage"] = df_amount["Rate Percentage"].astype(float)
    df_amount = df_amount.groupby(["CourseUniqueId", "Discount name"]).agg({
        "index": list,
        "Discount rate type": "first",
        "Amount": "sum",
        "Percentage of fee": "sum",
        "Rate Flat": "first",
        "Rate Percentage": "first"
    }).reset_index()

    # If value != discount amount -> error
    incorrect_amount_index: pd.Series = df_amount.loc[(df_amount["Discount rate type"] == "Flat")
    & (df_amount["Amount"].astype(str) != df_amount["Rate Flat"].astype(str))
    , "index"]
    incorrect_amount_index = incorrect_amount_index.explode().unique()
    incorrect_amount_mask: pd.Series = pd.Series(df_check.index.isin(incorrect_amount_index))
    mark_result(
        df=df_check,
        mask=incorrect_amount_mask,
        column="Discount rate type - Amount - Rate Flat",
        validation_type="Special logic",
        message="Discount rate type = Flat but financial discount amount != Rate Flat",
        extra_message="All discount rate type = Flat have financial discount amount = Rate Flat",
        sheet_name="CourseFinancialSetupDiscount"
    )

    # If value != discount percentage -> error
    incorrect_amount_index = df_amount.loc[(df_amount["Discount rate type"] == "Percentage")
    & (df_amount["Percentage of fee"].astype(str) != df_amount["Rate Percentage"].astype(str))
    , "index"]
    incorrect_amount_index = incorrect_amount_index.explode().unique()
    incorrect_amount_mask: pd.Series = pd.Series(df_check.index.isin(incorrect_amount_index))
    mark_result(
        df=df_check,
        mask=incorrect_amount_mask,
        column="Discount rate type - Percentage of fee - Rate Percentage",
        validation_type="Special logic",
        message="Discount rate type = Percentage but financial discount Percentage of fee != Rate Percentage",
        extra_message="All discount rate type = Percentage have financial discount Percentage of fee = Rate Percentage",
        sheet_name="CourseFinancialSetupDiscount"
    )
    return df_check


@logger_wrapper
def course_financial_setup_discount(df: pd.DataFrame, *args, **kwargs) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")


    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Discount
    df_discount = read_reference_data(
        finance_setup_file_path,
        "Discount",
        usecols=[
            "Discount name",
            "Discount rate type",
            "Rate Flat",
            "Rate Percentage"
        ]
    )

    # CourseFinancialSetupDiscount
    df_course_financial_revenue = read_reference_data(
        course_setup_file_path,
        "CourseFinancialSetupRevenue",
        usecols=[
            "CourseUniqueId",
            "Course/Product code/Module",
        ]
    )

    # CourseFeeSetupDiscount
    df_course_fee_setup_discount = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetupDiscount",
        usecols=[
            "CourseUniqueId",
            "Discount name",
        ]
    )


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # check id + dis + course/mode is enough, if lack -> issue, extra -> issue
    # Check course unique id, discount name
    check_discount_name(df_check, df_course_fee_setup_discount)

    # Check course unique id, course/product code/module
    check_modules(df_check, df_course_financial_revenue)

    check_financial_discount_and_fee_setup_discount(df_check, df_course_fee_setup_discount)

    df_check = df_check.merge(df_discount, on="Discount name", how="left")
    df_check = check_empty_amount_and_percentage(df_check)
    df_check = check_finacial_discount_vs_discount_setup(df_check)

    df["validation_result"] = df_check["validation_result"]

    return df

"""
@logger_wrapper
def check_finacial_discount_and_amount(df: pd.DataFrame, df_course_financial_revenue: pd.DataFrame) -> None:
    id_course_mods_revenue: set = set(zip(df_course_financial_revenue[["CourseUniqueId", "Course/Product code/Module"]]))
    id_course_mods_discount: set = set(zip(df[["CourseUniqueId", "Course/Product code/Module"]]))
    # check id + course/mod in CourseFinancialSetupRevenue but not in CourseFinancialSetupDiscount
    lack_of_discount = id_course_mods_revenue - id_course_mods_discount
    if len(lack_of_discount) > 0:
        logger.error(f"[CourseUniqueId] [Special logic] Some courses have Course/Product code/Module in CourseFinancialSetupRevenue but not in CourseFinancialSetupDiscount. Amount: {len(lack_of_discount)}. Details: {lack_of_discount}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All courses have Course/Product code/Module in CourseFinancialSetupRevenue are in CourseFinancialSetupDiscount")
    # check id + course/mod in CourseFinancialSetupDiscount but not in CourseFinancialSetupRevenue
    extra_discount = id_course_mods_discount - id_course_mods_revenue
    if len(extra_discount) > 0:
        logger.error(f"[CourseUniqueId] [Special logic] Some courses have Course/Product code/Module in CourseFinancialSetupDiscount but not in CourseFinancialSetupRevenue. Amount: {len(extra_discount)}. Details: {extra_discount}")
    else:
        logger.success("[CourseUniqueId] [Special logic] All courses have Course/Product code/Module in CourseFinancialSetupDiscount are in CourseFinancialSetupRevenue")
"""