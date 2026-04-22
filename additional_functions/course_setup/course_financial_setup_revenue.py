import pandas as pd
from typing import Optional, List
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data
from configs.constants import additional_report_folder_path


@logger_wrapper
def check_revenue_type(
    df_origin: pd.DataFrame, df_course_fee_setup: pd.DataFrame
) -> pd.DataFrame:
    """
    a. "Course Setup > CourseFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum", then select "Revenue type" is "Revenue"
    b. "Course Setup > CourseFeeSetup > Method to calculate" is "Tier based", then select "Revenue type" is "Revenue tier"
    c. "Course Setup > CourseFeeSetup > Method to calculate" is "Per intake", then create two entries for the same CourseUniqueId:
    - One with "Revenue type" is "Revenue per intake"
    - One with "Revenue type" is "Revenue per additional learner"
    """
    # a. "Course Setup > CourseFeeSetup > Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum", then select "Revenue type" is "Revenue"
    df = df_origin.copy().merge(df_course_fee_setup, on="CourseUniqueId", how="left")
    incorrect_revenue_type_a: pd.Series = (
        df["Method to calculate"].isin(["Per Pax Attendees", "Min. pax", "Lump sum"])
    ) & (df["Revenue type"] != "Revenue")

    mark_result(
        df=df,
        mask=incorrect_revenue_type_a,
        column="Revenue type",
        validation_type="Special logic",
        message='"Revenue type" is "Revenue" if "Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum"',
        extra_message='"Revenue type" is "Revenue" if "Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum"',
        sheet_name="CourseFinancialSetupRevenue",
    )

    # b. "Course Setup > CourseFeeSetup > Method to calculate" is "Tier based", then select "Revenue type" is "Revenue tier"
    incorrect_revenue_type_b: pd.Series = (
        df["Method to calculate"] == "Tier based"
    ) & (df["Revenue type"] != "Revenue tier")
    mark_result(
        df=df,
        mask=incorrect_revenue_type_b,
        column="Revenue type",
        validation_type="Special logic",
        message='"Revenue type" is "Revenue tier" if "Method to calculate" is "Tier based"',
        extra_message='"Revenue type" is "Revenue tier" if "Method to calculate" is "Tier based"',
        sheet_name="CourseFinancialSetupRevenue",
    )

    # c. "Course Setup > CourseFeeSetup > Method to calculate" is "Per intake", then create two entries for the same CourseUniqueId:
    # - One with "Revenue type" is "Revenue per intake"
    # - One with "Revenue type" is "Revenue per additional learner"
    # /Check invalid REvenue type
    invalid_revenue_type_c: pd.Series = (df["Method to calculate"] == "Per intake") & ~(
        df["Revenue type"].isin(
            ["Revenue per intake", "Revenue per additional learner"]
        )
    )
    mark_result(
        df,
        invalid_revenue_type_c,
        column="Revenue type",
        validation_type="Special logic",
        message='Revenue type must be "Revenue per intake" or "Revenue per additional learner" if Method to calculate is "Per intake"',
        extra_message='All "Revenue type" = "Revenue per intake" or "Revenue per additional learner" if Method to calculate is "Per intake"',
        sheet_name="CourseFinancialSetupRevenue",
    )

    # Check revenue is not enough: "Revenue per intake" or "Revenue per additional learner"
    df_check_type_c = (
        df.loc[df["Method to calculate"] == "Per intake"].copy().reset_index()
    )
    if df_check_type_c.empty:
        logger.info(
            f"[{check_revenue_type.__name__}]No course has Method to calculate = Per intake to check enough revenue type."
        )
        return df

    grouped = df_check_type_c.groupby("CourseUniqueId").agg(
        {"Revenue type": list, "index": list}
    )
    required = {"Revenue per intake", "Revenue per additional learner"}

    # Find groups where the set of types is not a superset of 'required'
    invalid_ids = grouped[
        grouped["Revenue type"].map(lambda x: not required.issubset(set(x)))
    ]

    if not invalid_ids.empty:
        # Explode the list of original indices to mark every row for that CourseUniqueId
        all_bad_indices = [idx for sublist in invalid_ids["index"] for idx in sublist]

        mask_c = df.index.isin(all_bad_indices)

        mark_result(
            df=df,
            mask=mask_c,
            column="Revenue type",
            validation_type="Special logic",
            message='CourseUniqueId having Method to calculate = Per intake must have BOTH "Revenue per intake" and "Revenue per additional learner" entries.',
            extra_message='CourseUniqueIds having Method to calculate = Per intake have BOTH "Revenue per intake" and "Revenue per additional learner" entries.',
            sheet_name="CourseFinancialSetupRevenue",
        )

    df_origin["validation_result"] = df["validation_result"]
    return df_origin


@logger_wrapper
def check_course_product_module(
    df_origin: pd.DataFrame, df_course: pd.DataFrame
) -> pd.DataFrame:
    """
    Check enough course/module/product code or not
    """
    df = df_origin.copy().reset_index()
    df = (
        df.groupby("CourseUniqueId")
        .agg({"Course/Product code/Module": list})
        .reset_index()
    )
    df["Course/Product code/Module"] = df["Course/Product code/Module"].map(
        lambda x: set(x) if x else set()
    )

    df = df.merge(df_course, on="CourseUniqueId", how="left")
    df["All modules"] = df["All modules"].map(lambda x: set(x) if x else set())
    df["Missing data"] = df.apply(
        lambda x: x["All modules"] - x["Course/Product code/Module"], axis=1
    )
    is_missing_data: pd.Series = df["Missing data"].map(lambda x: len(x) > 0)
    df_result = df.loc[is_missing_data]
    if not df_result.empty:
        df_result = df_result.reset_index(drop=True)
        df_result.index = df_result.index + 1
        df_result = df_result[["CourseUniqueId", "Missing data"]]
        df_result["Missing data"] = df_result["Missing data"].map(
            lambda x: ", ".join(x)
        )
        logger.error(
            f"[CourseFinancialSetupRevenue - Course/Product code/Module] [Special logic] Course is lack of Course/Product code/Module. Amount: {df_result.shape[0]}"
        )
        if df_result.shape[0] <= 10:
            logger.error(
                f"[CourseFinancialSetupRevenue - Course/Product code/Module] [Special logic] Details:\n{df_result.to_markdown()}"
            )
        else:
            detail_report_file_path: str = f"{additional_report_folder_path}/CourseFinancialSetupRevenue missing modules.xlsx"
            df_result.to_excel(
                detail_report_file_path,
                sheet_name="CourseFinancialSetupRevenue",
                index=False,
            )
            logger.error(
                f"[CourseFinancialSetupRevenue - Module code] [Special logic] More than 10 error data. Detail error in: {detail_report_file_path}"
            )
    else:
        logger.info(
            "[CourseFinancialSetupRevenue - Course/Product code/Module] [Special logic] All course have enough Course/Product code/Module"
        )

    df2 = df_origin.copy().reset_index()
    df2 = df2.merge(df_course, on="CourseUniqueId", how="left")
    is_extra_modules_mask: pd.Series = (
        df2["Course/Product code/Module"].notna()
        & df2["All modules"].notna()
        & (
            df2.apply(
                lambda x: x["Course/Product code/Module"] not in x["All modules"],
                axis=1,
            )
        )
    )
    mark_result(
        df=df_origin,
        mask=is_extra_modules_mask,
        column="Course/Product code/Module",
        validation_type="Special logic",
        message="Extra Course/Product code/Module",
        extra_message="All Courses/Product codes/Modules are in refer sheets",
        sheet_name="CourseFinancialSetupRevenue",
    )

    return df_origin


@logger_wrapper
def check_revenue_completeness(
    df_origin: pd.DataFrame, df_course: pd.DataFrame
) -> pd.DataFrame:
    """
    Checks if a CourseUniqueId has all required records:
    - Blended: Must have entries for every Go1/E-Learning, Product, AND Module.
    - Normal: Must have entries for every Module.
    """

    # 1. Prepare Expected References from Course Meta
    def get_expected_set(row):
        expected = set()
        # For Blended, we check all buckets
        cols_to_check = (
            ["Selected courses", "Module code"]
            if row["Blended course"] == "Yes"
            else ["Module code"]
        )

        for col in cols_to_check:
            val = row.get(col)
            if isinstance(val, list):
                expected.update(map(str, val))
            elif pd.notna(val):
                expected.add(str(val))
        return expected

    df_course_copy = df_course.copy()
    df_course_copy["expected_refs"] = df_course_copy.apply(get_expected_set, axis=1)

    # 2. Aggregate Actual References from the Revenue DF
    actual_grouped = (
        df_origin.groupby("CourseUniqueId")["Course/Product code/Module"]
        .apply(lambda x: set(map(str, x.dropna())))
        .reset_index(name="actual_refs")
    )

    # 3. Merge and Compare
    check_df = actual_grouped.merge(
        df_course_copy[["CourseUniqueId", "expected_refs"]],
        on="CourseUniqueId",
        how="left",
    )

    # 4. Identification of Mismatches
    # Error if Actual is not EXACTLY the same as Expected
    check_df["is_mismatch"] = check_df.apply(
        lambda x: (
            x["actual_refs"] != x["expected_refs"]
            if isinstance(x["expected_refs"], set)
            else False
        ),
        axis=1,
    )

    # 5. Map Errors back to df_origin
    invalid_ids = check_df.loc[check_df["is_mismatch"], "CourseUniqueId"].unique()
    mask = df_origin["CourseUniqueId"].isin(invalid_ids)
    mark_result(
        df=df_origin,
        mask=mask,
        column="CourseUniqueId",
        validation_type="Completeness",
        message="Missing or Extra records. Ensure every Course/Product/Module from Setup has a corresponding row here",
        extra_message="CourseUniqueId has all required records: Blended: have entries for every Go1/E-Learning, Product, AND Module. Others: have entries for every Module",
        sheet_name="CourseFinancialSetupRevenue",
    )

    return df_origin


@logger_wrapper
def check_sum_of_amount(
    df_origin: pd.DataFrame, df_course_fee_setup: pd.DataFrame
) -> pd.DataFrame:
    # Amount must be the same as:
    # - Course module - Flat rate if course level
    # - Total amount of all modules if module level
    # - Total amount of all tiers if having course tier rate fee
    # - Sum of "Charge rate of (without GST) per additional learner" if revenue type == Revenue per additional learner
    df = df_origin.copy().reset_index()
    df["Amount"] = df["Amount"].astype(float)
    df = (
        df.groupby(["CourseUniqueId", "Revenue type"])
        .agg({"Amount": "sum", "index": list})
        .reset_index()
    )
    df = df.merge(df_course_fee_setup, on="CourseUniqueId", how="left")
    df["Amount"] = df["Amount"].astype(float).round(14)

    # Revenue type = Revenue
    invalid_sum_of_amount_revenue_mask: pd.Series = (
        (df["Revenue type"] == "Revenue")
        & (df["Sum of flat rate"].fillna(0).round(14) != df["Amount"])
    ) & (
        (df["Revenue type"] == "Revenue")
        & (df["Sum of module rate"].fillna(0).round(14) != df["Amount"])
    )
    invalid_course = df.loc[
        invalid_sum_of_amount_revenue_mask, "CourseUniqueId"
    ].unique()
    invalid_sum_of_amount_mask = df_origin["CourseUniqueId"].isin(invalid_course) & (
        df_origin["Revenue type"] == "Revenue"
    )
    mark_result(
        df=df_origin,
        mask=invalid_sum_of_amount_mask,
        column="Amount",
        validation_type="Special logic",
        message="Amount must be the same as: Course module - Flat rate if course level; Total amount of all modules if module level",
        extra_message="All amount are the same as: Course module - Flat rate if course level; Total amount of all modules if module level",
        sheet_name="CourseFinancialSetupRevenue",
    )

    # Revenue type = Revenue tier
    invalid_sum_of_amount_revenue_tier_mask: pd.Series = (
        df["Revenue type"] == "Revenue tier"
    ) & (df["Sum of tier rate"].fillna(0).round(14) != df["Amount"])
    invalid_course = df.loc[
        invalid_sum_of_amount_revenue_tier_mask, "CourseUniqueId"
    ].unique()
    invalid_sum_of_amount_mask = df_origin["CourseUniqueId"].isin(invalid_course) & (
        df_origin["Revenue type"] == "Revenue tier"
    )
    mark_result(
        df=df_origin,
        mask=invalid_sum_of_amount_mask,
        column="Amount",
        validation_type="Special logic",
        message="Amount must be the same as: Total amount of all tiers if having course tier rate fee",
        extra_message="All amount are the same as: Total amount of all tiers if having course tier rate fee",
        sheet_name="CourseFinancialSetupRevenue",
    )

    # Revenue type = Revenue per additional learner
    invalid_sum_of_amount_revenue_per_additional_learner_mask: pd.Series = (
        df["Revenue type"] == "Revenue per additional learner"
    ) & (df["Sum of charge rate"].fillna(0).round(14) != df["Amount"])
    invalid_course = df.loc[
        invalid_sum_of_amount_revenue_per_additional_learner_mask, "CourseUniqueId"
    ].unique()
    invalid_sum_of_amount_mask = df_origin["CourseUniqueId"].isin(invalid_course) & (
        df_origin["Revenue type"] == "Revenue per additional learner"
    )
    mark_result(
        df=df_origin,
        mask=invalid_sum_of_amount_mask,
        column="Amount",
        validation_type="Special logic",
        message="Amount must be the same as: Sum of 'Charge rate of (without GST) per additional learner' if revenue type == Revenue per additional learner",
        extra_message="All amount are the same as: Sum of 'Charge rate of (without GST) per additional learner' if revenue type == Revenue per additional learner",
        sheet_name="CourseFinancialSetupRevenue",
    )

    # Revenue type = Revenue per intake
    invalid_sum_of_amount_revenue_per_intake_mask: pd.Series = (
        df["Revenue type"] == "Revenue per intake"
    ) & (df["Sum of package rate of"].fillna(0).round(14) != df["Amount"])
    invalid_course = df.loc[
        invalid_sum_of_amount_revenue_per_intake_mask, "CourseUniqueId"
    ].unique()
    invalid_sum_of_amount_mask = df_origin["CourseUniqueId"].isin(invalid_course) & (
        df_origin["Revenue type"] == "Revenue per intake"
    )
    mark_result(
        df=df_origin,
        mask=invalid_sum_of_amount_mask,
        column="Amount",
        validation_type="Special logic",
        message="Amount must be the same as: Sum of 'Package rate of (without GST)' if revenue type == Revenue per intake",
        extra_message="All amount are the same as: Sum of 'Package rate of (without GST)' if revenue type == Revenue per intake",
        sheet_name="CourseFinancialSetupRevenue",
    )

    return df_origin


@logger_wrapper
def course_financial_setup_revenue(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    # Calculate course fee setup total amount
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")

    # ----------------------------------------
    # Read reference data
    # ----------------------------------------
    rate_cols: List = [
        "Flat rate per pax (without GST)",
        "Package rate (without GST)",
        "Package rate of (without GST)",
        "Charge rate of (without GST) per additional learner",
    ]
    df_course_fee_setup = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=["CourseUniqueId", "Method to calculate", "Setup fee at"] + rate_cols,
    )
    course_fee_setup_ids: pd.Series = df_course_fee_setup["CourseUniqueId"].unique()
    df_course_fee_setup = df_course_fee_setup.loc[
        df_course_fee_setup["CourseUniqueId"].isin(df["CourseUniqueId"])
    ]
    df_course_fee_setup[rate_cols] = df_course_fee_setup[rate_cols].astype(float)
    df_course_fee_setup["Sum of charge rate"] = df_course_fee_setup[
        "Charge rate of (without GST) per additional learner"
    ].fillna(0)
    df_course_fee_setup["Sum of package rate of"] = df_course_fee_setup[
        "Package rate of (without GST)"
    ].fillna(0)
    df_course_fee_setup.loc[
        (
            (df_course_fee_setup["Setup fee at"] != "Course level")
            & (df_course_fee_setup["Method to calculate"] == "Per Pax Attendees")
        ),
        rate_cols,
    ] = 0
    df_course_fee_setup["Sum of flat rate"] = df_course_fee_setup[rate_cols].sum(axis=1)
    df_course_fee_setup = df_course_fee_setup.drop(columns=rate_cols)

    # Merge module and e learning module for course
    df_course = read_reference_data(
        course_setup_file_path,
        "Course",
        usecols=["CourseUniqueId", "Blended course", "Status"],
    )
    course_ids: pd.Series = df_course.loc[
        df_course["Status"].isin(
            ["Pending activation", "Scheduled activation", "Active", "Inactive"]
        ),
        "CourseUniqueId",
    ].unique()
    df_course = df_course.loc[df_course["CourseUniqueId"].isin(df["CourseUniqueId"])]
    df_course = df_course.merge(df_course_fee_setup, on="CourseUniqueId", how="left")

    df_blended = read_reference_data(
        course_setup_file_path,
        "ELearningCourseConfiguration",
        usecols=["CourseUniqueId", "Selected courses"],
    )
    df_course = df_course.merge(df_blended, on="CourseUniqueId", how="left")

    df_pathway = read_reference_data(
        course_setup_file_path, "Pathway", usecols=["CourseUniqueId", "Pathway ID"]
    )
    df_course = df_course.merge(df_pathway, on="CourseUniqueId", how="left")

    df_pathway_structure = read_reference_data(
        course_setup_file_path,
        "PathwayStructure",
        usecols=["Pathway ID", "Module code"],
    )
    df_course = df_course.merge(df_pathway_structure, on="Pathway ID", how="left")
    df_blended_course = df_course.drop(columns=["Pathway ID"])
    df_blended_course = df_blended_course.loc[
        df_blended_course["Blended course"] == "Yes"
    ]

    df_blended_course = (
        df_blended_course.groupby("CourseUniqueId")
        .agg({"Blended course": "first", "Selected courses": list, "Module code": list})
        .reset_index()
    )

    df_course_check = (
        df_course.groupby("CourseUniqueId")
        .agg({"Selected courses": list, "Module code": list})
        .reset_index()
    )
    df_course_check["Selected courses"] = df_course_check["Selected courses"].map(
        lambda x: [] if not isinstance(x, list) else x
    )
    df_course_check["Module code"] = df_course_check["Module code"].map(
        lambda x: [] if not isinstance(x, list) else x
    )
    df_course_check["All modules"] = (
        df_course_check["Selected courses"] + df_course_check["Module code"]
    )
    df_course_check["All modules"] = df_course_check["All modules"].map(
        lambda x: set([i for i in x if pd.notna(i) and i != ""])
    )
    df_course_check = df_course_check[["CourseUniqueId", "All modules"]]

    # ----------------------------------------
    # Validation
    # ----------------------------------------
    # CourseUniqueId in Course but not in CourseFinancialSetupRevenue
    missing_course_ids: set = set(course_ids) - set(df["CourseUniqueId"])
    if len(missing_course_ids) > 0:
        logger.error(
            f"[CourseFinancialSetupRevenue - CourseUniqueId] [Special logic] CourseUniqueId in Course but not in CourseFinancialSetupRevenue. Amount: {len(missing_course_ids)}. Details: {missing_course_ids}"
        )
    else:
        logger.info(
            "[CourseFinancialSetupRevenue - CourseUniqueId] [Special logic] All CourseUniqueId in Course are in CourseFinancialSetupRevenue."
        )

    # CourseUniqueId in CourseFeeSetup but not in CourseFinancialSetupRevenue
    missing_course_fee_setup_ids: set = set(course_fee_setup_ids) - set(
        df["CourseUniqueId"]
    )
    if len(missing_course_fee_setup_ids) > 0:
        logger.error(
            f"[CourseFinancialSetupRevenue - CourseUniqueId] [Special logic] CourseUniqueId in CourseFeeSetup but not in CourseFinancialSetupRevenue. Amount: {len(missing_course_fee_setup_ids)}. Details: {missing_course_fee_setup_ids}"
        )
    else:
        logger.info(
            "[CourseFinancialSetupRevenue - CourseUniqueId] [Special logic] All CourseUniqueId in CourseFeeSetup are in CourseFinancialSetupRevenue."
        )

    df = check_revenue_type(df, df_course_fee_setup)
    df = check_course_product_module(df, df_course_check)
    df = check_revenue_completeness(df, df_blended_course)

    # Sum of amount fee of all modules, all tiers
    df_course_module_fee_rate = read_reference_data(
        course_setup_file_path, "CourseModuleFeeRate"
    )
    df_course_module_fee_rate = df_course_module_fee_rate.loc[
        df_course_module_fee_rate["CourseUniqueId"].isin(df["CourseUniqueId"])
    ]
    df_course_module_fee_rate = df_course_module_fee_rate.rename(
        columns={"Module fee per pax (without GST)": "Sum of module rate"}
    )
    df_course_module_fee_rate["Sum of module rate"] = df_course_module_fee_rate[
        "Sum of module rate"
    ].astype(float)
    df_course_module_fee_rate = (
        df_course_module_fee_rate.groupby("CourseUniqueId")
        .agg({"Sum of module rate": "sum"})
        .reset_index()
    )
    df_course = df_course.merge(
        df_course_module_fee_rate, on="CourseUniqueId", how="left"
    )

    df_course_tier_rate_fee = read_reference_data(
        course_setup_file_path, "CourseTierRateFee"
    )
    df_course_tier_rate_fee = df_course_tier_rate_fee.loc[
        df_course_tier_rate_fee["CourseUniqueId"].isin(df["CourseUniqueId"])
    ]
    df_course_tier_rate_fee = df_course_tier_rate_fee.rename(
        columns={"Flat rate per pax (without GST)": "Sum of tier rate"}
    )
    df_course_tier_rate_fee["Sum of tier rate"] = df_course_tier_rate_fee[
        "Sum of tier rate"
    ].astype(float)
    df_course_tier_rate_fee = (
        df_course_tier_rate_fee.groupby("CourseUniqueId")
        .agg({"Sum of tier rate": "sum"})
        .reset_index()
    )
    df_course = df_course.merge(
        df_course_tier_rate_fee, on="CourseUniqueId", how="left"
    )
    df_course = df_course.drop_duplicates("CourseUniqueId")
    df = check_sum_of_amount(df, df_course)

    # Revenue tier, must comare that amount of each tier - each module the same as in course tier rate fee

    # df = df.drop(columns=["Method to calculate", "Sum of flat rate", "Sum of charge rate", "Sum of package rate of"])
    return df
