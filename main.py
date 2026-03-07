import datetime
import pandas as pd
import numpy as np
from loguru import logger
from typing import Dict
from pipeline.setup import read_file_strategy_factory
from utils import process_config, process_result
from pipeline.validate import validate_data
from pipeline.processing import process_data
from pipeline.setup import write_data_strategy_factory


# ========== Constants ==========
data_file_path = r"data\test_data\API learner profile template Dev.xlsx"
sheet_name: str = "Edit learner profile result"

validation_config_file_path: str = r"configs\validation\learner_account_validation_config.yaml"
preprocessing_congfig_file_path: str = r"configs\processing\learner_account_processing_config.yaml"

common_kwargs = {
    "${SHEET_NAME}": sheet_name,
    "${FILE_PATH}": data_file_path,
    "${EMPTY_LIST}": ["", "nan", np.nan, pd.NA, None, "nan", "NA", "<NA>", "NAT", "null"],
    "now": datetime.datetime.now()
}
# logger.info(common_kwargs)


read_excel_file_strategy = read_file_strategy_factory.get_strategy("excel")
read_json_file_strategy = read_file_strategy_factory.get_strategy("json")
read_yaml_file_strategy = read_file_strategy_factory.get_strategy("yaml")

# ========== Load Enum data ==========
learner_config_data_file_path: str = r"configs\constants\learner_enum_data.json"
learner_config_data: Dict = read_json_file_strategy(learner_config_data_file_path).load()
# logger.info(learner_config_data)

learner_enum_data: Dict = learner_config_data
# logger.info(learner_enum_data)

# in this case, enum is post processing, meaning string data will be mapped before send to be
enum_mapping_kwargs: Dict = {
    "${GENDER_ENUM}": learner_enum_data.get("Gender", {}),
    "${RESIDENTIAL_STATUS_ENUM}": learner_enum_data.get("ResidentialStatus", {}),
    "${PASS_TYPE_ENUM}": learner_enum_data.get("PassType", {}),
    "${RACE_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("RaceCode", {})},
    "${COUNTRY_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("CountryCode", {})},
    "${NATIONALITY_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("NationalityCode", {})},
    "${RESINDETAL_ADDRESS_ENUM}": learner_enum_data.get("ResidentialAddress", {}),
    "${NO_FLOOR_OR_UNIT_NUMER_ENUM}": learner_enum_data.get("noFloorAndUnit", {}),
    "${EMPLOYED_ENUM}": learner_enum_data.get("IsEmployed", {}),
    "${VIP_ENUM}": learner_enum_data.get("VIP", {}),
    "${DESIGNATION_ENUM}": {i.get("name"): i.get("code") for i in learner_enum_data.get("Designation", {})},
    "${BASIC_SALARY_ENUM}": {i.get("name"): i.get("code") for i in learner_enum_data.get("BasicSalary", {})}
}
# logger.info(enum_mapping_kwargs)

value_list_kwargs: Dict = {
    "${GENDER}": list(learner_enum_data.get("Gender", {}).keys()),
    "${RESIDENTIAL_STATUS}": list(learner_enum_data.get("ResidentialStatus", {}).keys()),
    "${PASS_TYPE}": list(learner_enum_data.get("PassType", {}).keys()),
    "${RACE}": [i.get("name") for i in learner_enum_data.get("Race", {})],
    "${COUNTRY}": [i.get("name") for i in learner_enum_data.get("CountryCode", {})],
    "${NATIONALITY}": [i.get("name") for i in learner_enum_data.get("NationalityCode", {})],
}
# logger.info(value_list_kwargs)

# ========== Load pre processing config ==========
# Load validation config
preprocessing_config_data = read_yaml_file_strategy(preprocessing_congfig_file_path).load()

df_preprocessing_config = pd.DataFrame(preprocessing_config_data["files"])
df_preprocessing_config = process_config(df_preprocessing_config, {**common_kwargs, **enum_mapping_kwargs})


# ========== Load validation config ==========
validation_config = read_yaml_file_strategy(validation_config_file_path).load()

df = pd.DataFrame(validation_config["files"])
df = process_config(df, {**common_kwargs, **value_list_kwargs})
df.to_excel("df_learner_account_validation_config.xlsx", index=False)


# ========== Read data ==========
df_learner_account: pd.DataFrame = read_excel_file_strategy(data_file_path).load(sheet_name=sheet_name, dtype=str, engine="calamine")
# logger.info(df_learner_account.columns)


# ========== Pre Processing data ==========
df_preprocessing = df_preprocessing_config.loc[df_preprocessing_config["processing_type"] == "preprocessing"]
logger.info(df_preprocessing)
df_learner_account = process_data(df_learner_account, df_preprocessing)
df_preprocessing.to_excel("df_learner_account_preprocessing_config.xlsx", index=False)


# ========== Test validation: mandatory vs inner ref ==========
df_learner_account = validate_data(df_learner_account, df)


# ========== Process result and write to error report ==========
df_learner_account = process_result(df_learner_account, file_path="test_learner_account_validation.xlsx")


# # ========== Post prcoessing: Enum for send request ==========

df_postprocessing = df_preprocessing_config.loc[df_preprocessing_config["processing_type"] == "postprocessing"]
# logger.info(df_postprocessing)

df_learner_account = process_data(df_learner_account, df_postprocessing)
logger.info(df_learner_account)
write_data_strategy_factory.get_strategy("excel")().run(df=df_postprocessing, file_path="df_learner_account_postrocessing_config.xlsx")




# # process config file ok
# # pre processing ok
# # and run validation -> which can in validation_config.yaml -> common config
# Which special -> write special validation -> inherit ValidationStrategy
# # need do: Outer reference and special logic -> pipeline
#  data_type -> check integer type, numeric type, boolean type



# ===========================

# remove white space
# remove_white_space_columns = df_preprocessing_config.loc[df_preprocessing_config["type"] == "remove_white_space"]
# remove_white_space_processing = preprocessing_strategy_factory.get_strategy("remove_white_space")
# for index, config in remove_white_space_columns.iterrows():
#     df_learner_account = remove_white_space_processing(df=df_learner_account, **config).run()
    
# # lower case
# string_case_configs = df_preprocessing_config.loc[df_preprocessing_config["type"] == "string_case"]
# string_case_processing = preprocessing_strategy_factory.get_strategy("string_case")
# for index, config in string_case_configs.iterrows():
#     df_learner_account = string_case_processing(df_learner_account, **config).run()

# # Fill default value: be carefull
# # 2 types:
# # no condition: can fill all
# # having condition: for example: Pass type justr fill default if Reasidental status is not Foreigner, if is Foreigner, it will be validation case: pass type can not empty if Reasidental status is Foreigner
# fill_default_configs = df_preprocessing_config.loc[df_preprocessing_config["type"] == "fill_default"]
# fill_default_processing = preprocessing_strategy_factory.get_strategy("fill_default")(df_learner_account)
# for index, config in fill_default_configs.iterrows():
#     column = config["name"]
#     default_value = config["default_value"]
#     df_learner_account = fill_default_processing.run(column=column, default_value=default_value)

# Split string (Don't have)


# mandatory_validation_strategy = validation_strategy_factory.get_strategy("mandatory") # can add more , validation_type="Check mandatory", message="This field is required." here if want

# for index, config in required_columns.iterrows():
#     validation_strategy_factory.get_strategy("mandatory")(
#         df=df_learner_account,
#         **config
#         ).run()


# unique_validation_strategy = validation_strategy_factory.get_strategy("unique")
# unique_columns.apply(
#     lambda x: validation_strategy_factory.get_strategy("unique")(
#     df=df_learner_account,
#     **x
#     ).run(),
#     axis=1
# )
# for index, config in unique_columns.iterrows():
#     validation_strategy_factory.get_strategy("unique")(
#     df=df_learner_account,
#     **config
#     ).run()


# inner_validation_strategy = validation_strategy_factory.get_strategy("inner_reference")
# inner_ref_columns = df.loc[df["ref_info"].notna()]
# for index, config in inner_ref_columns.iterrows():
#     result = inner_validation_strategy(
#         df=df_learner_account,
#         **config
#     ).run()


# # Date time format
# datetime_format_validation_strategy = validation_strategy_factory.get_strategy("datetime_format")
# datetime_format_columns = df.loc[df["type"] == "datetime_format"]

# for index, config in datetime_format_columns.iterrows():
#     datetime_format_validation_strategy(
#         df=df_learner_account,
#         **config
#     ).run()

# # Date time value
# in_range_datetime_validation_strategy = validation_strategy_factory.get_strategy("datetime_range")
# in_range_datetime_columns = df.loc[df["type"] == "datetime_range"]
# for index, config in in_range_datetime_columns.iterrows():
#     in_range_datetime_validation_strategy(
#         df=df_learner_account,
#         **config
#     ).run()


# # Value list
# value_list_validation_strategy = validation_strategy_factory.get_strategy("value_list")
# value_list_columns = df.loc[df["type"] == "value_list"]
# for index, config in value_list_columns.iterrows():
#     value_list_validation_strategy(
#         df=df_learner_account,
#         **config
#     ).run()

# logger.info(df_learner_account["validation_result"])
