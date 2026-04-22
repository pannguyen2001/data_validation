import pandas as pd
from typing import Optional, List
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


@logger_wrapper
def check_discount_name(df_check: pd.DataFrame, df_intake_fee_setup_discount: pd.DataFrame) -> None:
    df_intake_fee_setup_discount = df_intake_fee_setup_discount.groupby(["CourseIntakeId"]).agg({"Discount name": list}).reset_index()
    df_intake_fee_setup_discount["Discount name"] = df_intake_fee_setup_discount["Discount name"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())
    df_intake_fee_setup_discount = df_intake_fee_setup_discount.rename(columns={"Discount name": "All discounts"})

    # [Intake, discount name] is lacking in CourseIntakeFinancialDiscount: CourseIntakeFeeSetupDiscount has but CourseIntakeFinancialDiscount not
    df_check_discount = df_check.reset_index().groupby("CourseIntakeId").agg({"Discount name": list, "index": list}).reset_index()
    df_check_discount["Discount name"] = df_check_discount["Discount name"].map(lambda x: set(x) if isinstance(x, list) else set())

    df_check_discount = df_check_discount.merge(df_intake_fee_setup_discount, how="left", on="CourseIntakeId")
    df_check_discount["All discounts"] = df_check_discount["All discounts"].map(lambda x: x if isinstance(x, set) else set())
    df_check_discount["Discount name"] = df_check_discount["Discount name"].map(lambda x: x if isinstance(x, set) else set())

    df_check_discount["Missing discounts"] = df_check_discount.apply(lambda x: x["All discounts"] - x["Discount name"], axis=1)
    missing_discounts_mask: pd.Series = df_check_discount["Missing discounts"].map(lambda x: len(x) > 0)

    # [Intake, discount name] is extra in CourseIntakeFinancialDiscount : CourseIntakeFeeSetupDiscount does not have but CourseIntakeFinancialDiscount has
    df_check_discount["Extra discounts"] = df_check_discount.apply(lambda x: x["Discount name"] - x["All discounts"], axis=1)
    extra_discounts_mask: pd.Series = df_check_discount["Extra discounts"].map(lambda x: len(x) > 0)

    df_result = df_check_discount.loc[missing_discounts_mask | extra_discounts_mask, ['CourseIntakeId', "Missing discounts", "Extra discounts"]]
    if not df_result.empty:
        df_result["Missing discounts"] = df_result["Missing discounts"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result["Extra discounts"] = df_result["Extra discounts"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result = df_result.reset_index(drop=True)
        df_result.index = df_result.index + 1
        logger.error(f"[CourseIntakeFinancialSetupDiscount - [CourseIntakeId - Discount name]] [Special logic] Some courses have missing/extra Discount names in CourseIntakeFinancialSetupDiscount that in/not in CourseIntakeFeeSetupDiscount. Amount: {df_result.shape[0]}. Details: {df_result.to_markdown()}")
    else:
        logger.success("[CourseIntakeFinancialSetupDiscount - [CourseUniqueId - Discount name]] [Special logic] All courses have Discount name in CourseIntakeFinancialSetupDiscount are in CourseIntakeFeeSetupDiscount.")

    # missing_discounts_index: pd.Series = missing_discounts_intakes["index"].explode().unique()
    # missing_discounts_final_mask: pd.Series = pd.Series(df.index.isin(missing_discounts_index))
    # mark_result(
    #     df=df,
    #     mask=missing_discounts_final_mask,
    #     column="Discount name",
    #     validation_type="Special logic",
    #     message="Intake, discount name in CourseIntakeFeeSetupDiscount and CourseIntakeFinancialDiscount is not match",
    #     extra_message="All intake, discount name in CourseIntakeFeeSetupDiscount are in CourseIntakeFinancialDiscount",
    #     sheet_name="CourseIntakeFinancialDiscount"
    # )

    # extra_discounts_index: pd.Series = extra_discounts_intakes["index"].explode().unique()
    # extra_discounts_final_mask: pd.Series = pd.Series(df.index.isin(extra_discounts_index))
    # mark_result(
    #     df=df,
    #     mask=extra_discounts_final_mask,
    #     column="Discount name",
    #     validation_type="Special logic",
    #     message="Intake, discount name in CourseIntakeFinancialDiscount and CourseIntakeFeeSetupDiscount is not match",
    #     extra_message="All intake, discount name in CourseIntakeFinancialDiscount are in CourseIntakeFeeSetupDiscount",
    #     sheet_name="CourseIntakeFinancialDiscount"
    # )


@logger_wrapper
def check_modules(df_check: pd.DataFrame, df_intake_financial_revenue: pd.DataFrame) -> None:
    df_intake_financial_revenue = df_intake_financial_revenue.groupby("CourseIntakeId").agg({"Course/Product code/Module": list}).reset_index()
    df_intake_financial_revenue["Course/Product code/Module"] = df_intake_financial_revenue["Course/Product code/Module"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())
    df_intake_financial_revenue = df_intake_financial_revenue.rename(columns={"Course/Product code/Module": "All modules"})

    df_check_module = df_check.reset_index().groupby("CourseIntakeId").agg({"Course/Product code/Module": list, "index": list}).reset_index()
    df_check_module["Course/Product code/Module"] = df_check_module["Course/Product code/Module"].map(lambda x: set(x) if all(pd.notna(i) for i in x) else set())

    df_check_module = df_check_module.merge(df_intake_financial_revenue, how="left", on="CourseIntakeId")
    df_check_module["All modules"] = df_check_module["All modules"].map(lambda x: x if isinstance(x, set) else set())
    df_check_module["Course/Product code"] = df_check_module["Course/Product code/Module"].map(lambda x: x if isinstance(x, set) else set())

    # [] is lacking in CourseIntakeFinancialDiscount: CourseIntakeFinancialRevenue has but CourseIntakeFinancialDiscount not
    df_check_module["Missing modules"] = df_check_module.apply(lambda x: x["All modules"] - x["Course/Product code/Module"], axis=1)
    missing_modules_mask: pd.Series = df_check_module["Missing modules"].map(lambda x: (len(x) > 0) and (all(pd.notna(i) for i in x)))

    # [] is extra in CourseIntakeFinancialDiscount: : CourseIntakeFinancialRevenue does not have but CourseIntakeFinancialDiscount has
    df_check_module["Extra modules"] = df_check_module.apply(lambda x: x["Course/Product code/Module"] - x["All modules"], axis=1)
    extra_modules_mask: pd.Series = df_check_module["Extra modules"].map(lambda x: (len(x) > 0) and all(pd.notna(i) for i in x))

    df_result = df_check_module.loc[missing_modules_mask | extra_modules_mask, ['CourseIntakeId', "Missing modules", "Extra modules"]].reset_index(drop=True)
    if not df_result.empty:
        df_result["Missing modules"] = df_result["Missing modules"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result["Extra modules"] = df_result["Extra modules"].map(lambda x: "\n".join(x) if len(x) > 0 else "")
        df_result = df_result.reset_index(drop=True)
        df_result.index = df_result.index + 1
        logger.error(f"[CourseIntakeFinancialDiscount - [CourseIntakeId - Course/Product code/Module]] [Special logic] Some courses have missing/extra Course/Product code/Module in CourseIntakeFinancialDiscount that in/not in CourseIntakeFinancialRevenue. Amount: {df_result.shape[0]}. Details:\n{df_result.to_markdown()}")
    else:
        logger.success("[CourseIntakeFinancialDiscount - [CourseIntakeId - Course/Product code/Module]] [Special logic] CourseIntakeFinancialDiscount has no extra modules or missing that not in or in CourseIntakeFinancialRevenue.")

        # missing_modules_index: pd.Series = missing_modules_intakes["index"].explode().unique()
        # missing_modules_final_mask: pd.Series = pd.Series(df.index.isin(missing_modules_index))
        # mark_result(
        #     df=df,
        #     mask=missing_modules_final_mask,
        #     column="Course/Product code/Module",
        #     validation_type="Special logic",
        #     message="Intake, module in CourseIntakeFinancialDiscount is not match as CourseIntakeFinancialRevenue",
        #     extra_message="All intake, module in CourseIntakeFinancialDiscount are in CourseIntakeFinancialRevenue",
        #     sheet_name="CourseIntakeFinancialDiscount"
        # )

        # extra_modules_index: pd.Series = extra_modules_intakes["index"].explode().unique()
        # extra_modules_final_mask: pd.Series = pd.Series(df.index.isin(extra_modules_index))
        # mark_result(
        #     df=df,
        #     mask=extra_modules_final_mask,
        #     column="Course/Product code/Module",
        #     validation_type="Special logic",
        #     message="Intake, module in CourseIntakeFinancialDiscount is not match as CourseIntakeFinancialRevenue",
        #     extra_message="All intake, module in CourseIntakeFinancialDiscount are in CourseIntakeFinancialRevenue",
        #     sheet_name="CourseIntakeFinancialDiscount"
        # )


@logger_wrapper
def check_amount_and_percentage_fee(df_check: pd.DataFrame, df_discount: pd.DataFrame, df_intake_fee_setup_discount: pd.DataFrame):
    # if intake setup fee discount amount is not empty, check with CourseIntakeFeeSetupDiscount
    df_check[["Amount", "Percentage of fee"]] = df_check[["Amount", "Percentage of fee"]].fillna(0).astype(float)
    df_check = df_check.reset_index().groupby(["CourseIntakeId", "Discount name"]).agg({"Amount": "sum", "Percentage of fee": "sum", "index": list}).reset_index()
    df_check[["Amount", "Percentage of fee"]] = df_check[["Amount", "Percentage of fee"]].astype(float).round(4).astype(str)

    df_check = df_check.merge(df_intake_fee_setup_discount, how="left", on=["CourseIntakeId", "Discount name"])
    df_check = df_check.merge(df_discount, how="left", on="Discount name")

    invalid_amount_mask_1: pd.Series = (
        ((df_check["Rate Flat"].notna())
        # & (df_check_fee_setup_discount["Amount"].notna())
        & (df_check["Rate Flat"] != df_check["Amount"]))
        | (
            (df_check["Rate Percentage"].notna())
            # & (df_check_fee_setup_discount["Percentage of fee"].notna())
            & (df_check["Rate Percentage"] != df_check["Percentage of fee"])
        )
    )
    invalid_index_1: pd.Series = df_check.loc[invalid_amount_mask_1, "index"]
    invalid_index_1 = invalid_index_1.explode("index").unique()

    # if intake setup fee discount amount is empty, check from finance setup -> discount
    invalid_amount_mask_2: pd.Series = (
        (
            df_check["Rate Flat"].isna()
            # & (df_check_discount["Amount"].notna())
            & (
                df_check["Amount"] != df_check["Discount rate flat"]
            )
        )
        | (
            df_check["Rate Percentage"].isna()
            # & (df_check_discount["Percentage of fee"].notna())
            & (
                df_check["Percentage of fee"] != df_check["Discount rate percentage"]
            )
        )
    )
    invalid_index_2: pd.Series = df_check.loc[invalid_amount_mask_2, "index"]
    invalid_index_2 = invalid_index_2.explode("index").unique()

    return (invalid_index_1, invalid_index_2)


@logger_wrapper
def course_intake_financial_discount(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    df_check = df.copy()
    intake_file_path: str = kwargs.get("${COURSE_INTAKE_SETUP_AND_OTHERS_FILE_PATH}")
    finance_setup_file_path: str = kwargs.get("${FINANCE_SETUP_FILE_PATH}")


    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    # Course intake fee setup
    df_intake_financial_revenue: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeFinancialRevenue",
        usecols=[
            "CourseIntakeId",
            "Course/Product code/Module"
        ]
    )

    # Course intake fee setup discount
    df_intake_fee_setup_discount: pd.DataFrame = read_reference_data(
        intake_file_path,
        "CourseIntakeFeeSetupDiscount",
        usecols=[
            "CourseIntakeId",
            "Discount name",
            "Rate Flat",
            "Rate Percentage"
        ]
    )
    df_intake_fee_setup_discount[["Rate Flat", "Rate Percentage"]] = df_intake_fee_setup_discount[["Rate Flat", "Rate Percentage"]].fillna(0).astype(float).round(4).astype(str)

    # Discount
    df_discount: pd.DataFrame = read_reference_data(
        finance_setup_file_path,
        "Discount",
        usecols=[
            "Discount name",
            "Rate Flat",
            "Rate Percentage"
        ]
    )
    df_discount = df_discount.rename(columns={"Rate Flat": "Discount rate flat", "Rate Percentage": "Discount rate percentage"})
    df_discount[["Discount rate flat", "Discount rate percentage"]] = df_discount[["Discount rate flat", "Discount rate percentage"]].fillna(0).astype(float).round(4).astype(str)


    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # Intake, discount name in CourseIntakeFeeSetupDiscount and CourseIntakeFinancialDiscount is not match
    check_discount_name(df_check, df_intake_fee_setup_discount)

    # Intake, module in CourseIntakeFinancialDiscount is not match as CourseIntakeFinancialRevenue
    check_modules(df_check, df_intake_financial_revenue)

    # Check amount
    (invalid_index_1, invalid_index_2) = check_amount_and_percentage_fee(df_check, df_discount, df_intake_fee_setup_discount)

    invalid_index_mask_1: pd.Series = pd.Series(df.index.isin(invalid_index_1))
    mark_result(
        df=df,
        mask=invalid_index_mask_1,
        column="Amount - Percentage",
        validation_type="Special logic",
        message="Amount or Percentage of fee in CourseIntakeFinancialDiscount does not match as CourseIntakeFeeSetupDiscount",
        extra_message="Amount or Percentage of fee in CourseIntakeFinancialDiscount matches as CourseIntakeFeeSetupDiscount",
        sheet_name="CourseIntakeFinancialDiscount"
    )

    invalid_index_mask_2: pd.Series = pd.Series(df.index.isin(invalid_index_2))
    mark_result(
        df=df,
        mask=invalid_index_mask_2,
        column="Amount - Percentage",
        validation_type="Special logic",
        message="Amount or Percentage of fee in CourseIntakeFinancialDiscount does not match as Discount",
        extra_message="Amount or Percentage of fee in CourseIntakeFinancialDiscount matches as Discount",
        sheet_name="CourseIntakeFinancialDiscount"
    )

    return df