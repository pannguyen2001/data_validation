import pandas as pd
from typing import Optional, List
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data


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
        extra_message='"Revenue type" is "Revenue" if "Method to calculate" is "Per Pax Attendees, Min. pax, Lump sum"'
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
        extra_message='"Revenue type" is "Revenue tier" if "Method to calculate" is "Tier based"'
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
        extra_message='All "Revenue type" = "Revenue per intake" or "Revenue per additional learner" if Method to calculate is "Per intake"'
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
            extra_message='CourseUniqueIds having Method to calculate = Per intake have BOTH "Revenue per intake" and "Revenue per additional learner" entries.'
        )

    df_origin["validation_result"] = df["validation_result"]
    return df_origin


@logger_wrapper
def check_course_product_module(
    df_origin: pd.DataFrame, df_course: pd.DataFrame
) -> pd.DataFrame:
    """
    1. The Course/Product code/Module reference follow this:
    a. "Course Setup > Course > Blended course"  is "Yes"
    In-house/Go1/Linkedin Learning - Refer to: Course Setup > ELearningCourseConfiguration > Selected courses
    Product Subscription - Refer to: Course Setup > Course  >Products
    Module Course - Refer to: Course Setup > PathwayStructure > Module code
    b. "Course Setup > Course > Blended course"  is "No"
    Module Course - Refer to: Course Setup > PathwayStructure > Module code
    3. Example:
    a. A blended course consists of 2 Go1 courses and 1 module; therefore, 3 records should be created in CourseFinancialSetupRevenue (one for each)
    b. A normal course with 2 modules should have 2 records created in CourseFinancialSetupRevenue (one for each module).
    """
    df = df_origin.copy()
    df = df.merge(df_course, on="CourseUniqueId", how="left")

    # a. "Course Setup > Course > Blended course"  is "Yes"
    # - In-house/Go1/Linkedin Learning : Refer to: Course Setup > ELearningCourseConfiguration > Selected courses
    # - Product Subscription: Refer to: Course Setup > Course  >Products
    # Module Course: Refer to: Course Setup > PathwayStructure > Module code
    # course_product_module_value_list: list[str] = set(df_course["Selected courses"].unique()).union(set(df_course["Module code"].unique()))

    invalid_type_a_mask: pd.Series = (df["Blended course"] == "Yes") & (
        df.apply(
            lambda x: (
                x["Course/Product code/Module"] not in x["Selected courses"]
                if pd.notna(x["Selected courses"])
                else False
            ),
            axis=1,
        )
        & (
            df.apply(
                lambda x: (
                    x["Course/Product code/Module"] not in x["Module code"]
                    if pd.notna(x["Module code"])
                    else False
                ),
                axis=1,
            )
        )
    )
    mark_result(
        df=df,
        mask=invalid_type_a_mask,
        column="Course/Product code/Module",
        validation_type="Special logic",
        message='Course/Product code/Module follow this: "Course Setup > Course > Blended course"  is "Yes" In-house/Go1/Linkedin Learning - Refer to: Course Setup > ELearningCourseConfiguration > Selected courses Product Subscription - Refer to: Course Setup > Course  >Products Module Course - Refer to: Course Setup > PathwayStructure > Module code',
        extra_message='All "Course/Product code/Module" follow this: "Course Setup > Course > Blended course"  is "Yes" In-house/Go1/Linkedin Learning - Refer to: Course Setup > ELearningCourseConfiguration > Selected courses Product Subscription - Refer to: Course Setup > Course  >Products Module Course - Refer to: Course Setup > PathwayStructure > Module code'
    )

    df_origin["validation_result"] = df["validation_result"]
    return df_origin


# @logger_wrapper
# def check_course_product_module(df_origin: pd.DataFrame, df_course: pd.DataFrame) -> pd.DataFrame:
#     df = df_origin.copy().merge(df_course, on="CourseUniqueId", how="left")

#     def validate_reference(row):
#         val = row.get("Course/Product code/Module")
#         if pd.isna(val):
#             return False # Skip empty; handled by 'Required' rule

#         # 1. Map 'Blended' status to the columns we are allowed to check
#         # 'Yes' -> Check all 3 sources | 'No' -> Check only Module code
#         check_cols = ["Selected courses", "Products", "Module code"] if row["Blended course"] == "Yes" else ["Module code"]

#         # 2. Build the unified Allowed Set for this specific row
#         allowed_refs = set()
#         for col in check_cols:
#             source = row.get(col)
#             if isinstance(source, list):
#                 allowed_refs.update(map(str, source))
#             elif pd.notna(source):
#                 allowed_refs.add(str(source))

#         # 3. Valid if 'val' is found in any of the allowed sources
#         return str(val) not in allowed_refs

#     # Single Pass: Create one mask for all rows
#     invalid_mask = df.apply(validate_reference, axis=1)

#     mark_result(
#         df=df,
#         mask=invalid_mask,
#         column="Course/Product code/Module",
#         validation_type="Special logic",
#         message='Reference mismatch: Blended="Yes" requires ELearning/Product/Module match; Blended="No" requires Module match.',
#     )

#     df_origin["validation_result"] = df["validation_result"]
#     return df_origin


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
        message="Missing or Extra records. Ensure every Course/Product/Module from Setup has a corresponding row here.",
        extra_message="CourseUniqueId has all required records: Blended: have entries for every Go1/E-Learning, Product, AND Module. Others: have entries for every Module"
    )

    return df_origin


@logger_wrapper
def check_sum_of_amount(
    df_origin: pd.DataFrame, df_course_fee_setup: pd.DataFrame
) -> pd.DataFrame:
    df = df_origin.copy().reset_index()
    df["Amount"] = df["Amount"].astype(float)
    df = (
        df.groupby("CourseUniqueId").agg({"Amount": "sum", "index": list}).reset_index()
    )
    df = df.merge(df_course_fee_setup, on="CourseUniqueId", how="left")
    df["Amount"] = df["Amount"].astype(float).round(14)

    invalid_sum_of_amount_mask: pd.Series = (
        (
            (df["Sum of flat rate"].notna())
            & (df["Sum of flat rate"].round(14) == df["Amount"])
        )
        | (
            (df["Sum of module rate"].notna())
            & (df["Sum of module rate"].round(14) == df["Amount"])
        )
        | (
            (df["Sum of tier rate"].notna())
            & (df["Sum of tier rate"].round(14) == df["Amount"])
        )
    )
    invalid_course = df.loc[invalid_sum_of_amount_mask, "CourseUniqueId"].unique()
    invalid_sum_of_amount_mask = ~df_origin["CourseUniqueId"].isin(invalid_course)
    mark_result(
        df=df_origin,
        mask=invalid_sum_of_amount_mask,
        column="Amount",
        validation_type="Special logic",
        message="Amount is the same as course fee setup, if course level, or sum of amount of all modules, if module level or sum of all tiers",
        extra_message="Amount is the same as course fee setup, if course level, or sum of amount of all modules, if module level or sum of all tiers"
    )

    return df_origin


@logger_wrapper
def course_financial_setup_revenue(
    df: pd.DataFrame, *args, **kwargs
) -> Optional[pd.DataFrame]:
    course_setup_file_path: str = kwargs.get("${COURSE_SETUP_FILE_PATH}")
    rate_cols: List = [
            "Flat rate per pax (without GST)",
            "Package rate (without GST)",
            "Package rate of (without GST)",
            "Charge rate of (without GST) per additional learner",
        ]
    df_course_fee_setup = read_reference_data(
        course_setup_file_path,
        "CourseFeeSetup",
        usecols=["CourseUniqueId", "Method to calculate"] + rate_cols,
    )
    df_course_fee_setup = df_course_fee_setup.loc[
        df_course_fee_setup["CourseUniqueId"].isin(df["CourseUniqueId"])
    ]
    df_course_fee_setup[rate_cols] = df_course_fee_setup[rate_cols].astype(float)
    # df_course_fee_setup = df_course_fee_setup.rename(columns={"Flat rate per pax (without GST)": "Sum of flat rate"})
    df_course_fee_setup["Sum of flat rate"] = df_course_fee_setup[rate_cols].sum(axis=1)
    df_course_fee_setup = df_course_fee_setup.drop(columns=rate_cols)

    df_course = read_reference_data(
        course_setup_file_path, "Course", usecols=["CourseUniqueId", "Blended course"]
    )
    df_course = df_course.loc[df_course["CourseUniqueId"].isin(df["CourseUniqueId"])]
    df_course = df_course.merge(df_course_fee_setup, on="CourseUniqueId", how="left")

    df_blended_course = read_reference_data(
        course_setup_file_path,
        "ELearningCourseConfiguration",
        usecols=["CourseUniqueId", "Selected courses"],
    )
    df_course = df_course.merge(df_blended_course, on="CourseUniqueId", how="left")

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

    # df = check_revenue_type(df, df_course_fee_setup)
    # df = check_course_product_module(df, df_blended_course)
    # df = check_revenue_completeness(df, df_blended_course)

    # Amount is the same as course fee setup, if course level, or sum of amount of all modules, if module level
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

    return df
