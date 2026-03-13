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
from pipeline.pipeline import data_validation_pipeline
from configs.constants import report_folder_path, date_today, time_today, DATA_FOLDER_PATH, CONSTANTS_CONFIG_FOLDER_PATH, PROCESSING_CONFIG_FOLDER_PATH, VALIDATION_CONFIG_FOLDER_PATH


# ========== Constants ==========
data_file_path = r"data/test_data/Data Template M02_Master Data Setup_C2R1.xlsx"
file_name: str = data_file_path.split("/")[-1].split(".")[0]
# r"data/test_data/API learner profile template Dev.xlsx"
# r"data/test_data/API learner profile template Dev - correct data.xlsx"
sheet_name: str = "InternalSuspendedList"
# "Edit learner profile result"

validation_config_file_path: str = r"configs/validation/master_data_setup_validation_config.yaml"
# r"configs/validation/learner_account_validation_config.yaml"
processing_congfig_file_path: str = "configs/processing/master_data_setup_processing_config.yml"
r"configs/processing/learner_account_processing_config.yaml"
# learner_config_data_file_path: str = r"configs/constants/learner_enum_data.json"

# process all config file, processing, validation file before run test
common_kwargs = {
    "${DATA_FOLDER_PATH}": DATA_FOLDER_PATH,
    "${SHEET_NAME}": sheet_name,
    "${FILE_PATH}": data_file_path,
    "${EMPTY_LIST}": ["", "nan", np.nan, pd.NA, None, "nan", "NA", "<NA>", "NAT", "null"],
    "now": datetime.datetime.now()
}

with open("/home/user/datavalidation/configs/file_info.yaml", "r") as f:
    import yaml
    data = yaml.load(f, Loader=yaml.FullLoader)
    df_file_info = pd.DataFrame(data)

df_file_info = df_file_info.explode("sheets", ignore_index=True)

file_name_mapping = df_file_info[["file_name", "file_name_mapping"]].dropna().drop_duplicates().to_dict(orient="records")

for item in file_name_mapping:
    key = "${" + item["file_name_mapping"] + "}"
    common_kwargs[key] = item["file_name"]

# logger.info(common_kwargs)

df_file_info["file_name"] = DATA_FOLDER_PATH + "/" + df_file_info["file_name"]
df_file_info["config_file"] = CONSTANTS_CONFIG_FOLDER_PATH + "/" + df_file_info["config_file"]
df_file_info["processing_config_file"] = PROCESSING_CONFIG_FOLDER_PATH + "/" + df_file_info["processing_config_file"]
df_file_info["validation_config_file"] = VALIDATION_CONFIG_FOLDER_PATH + "/" + df_file_info["validation_config_file"]

# ========== ReadFileStrategy ==========
read_excel_file_strategy = read_file_strategy_factory.get_strategy("excel")
read_json_file_strategy = read_file_strategy_factory.get_strategy("json")
read_yaml_file_strategy = read_file_strategy_factory.get_strategy("yaml")


# ========== Load constant data (enum value, real value list,...) ==========
# learner_config_data: Dict = read_json_file_strategy(learner_config_data_file_path).load()
# logger.info(learner_config_data)

# learner_enum_data: Dict = learner_config_data
# logger.info(learner_enum_data)

# enum is post processing, meaning string data will be mapped before send to be
# enum_mapping_kwargs: Dict = {
#     "${GENDER_ENUM}": learner_enum_data.get("Gender", {}),
#     "${RESIDENTIAL_STATUS_ENUM}": learner_enum_data.get("ResidentialStatus", {}),
#     "${PASS_TYPE_ENUM}": learner_enum_data.get("PassType", {}),
#     "${RACE_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("RaceCode", {})},
#     "${COUNTRY_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("CountryCode", {})},
#     "${NATIONALITY_ENUM}": {i.get("name"): i.get("id") for i in learner_enum_data.get("NationalityCode", {})},
#     "${RESINDETAL_ADDRESS_ENUM}": learner_enum_data.get("ResidentialAddress", {}),
#     "${NO_FLOOR_OR_UNIT_NUMER_ENUM}": learner_enum_data.get("noFloorAndUnit", {}),
#     "${EMPLOYED_ENUM}": learner_enum_data.get("IsEmployed", {}),
#     "${VIP_ENUM}": learner_enum_data.get("VIP", {}),
#     "${DESIGNATION_ENUM}": {i.get("name"): i.get("code") for i in learner_enum_data.get("Designation", {})},
#     "${BASIC_SALARY_ENUM}": {i.get("name"): i.get("code") for i in learner_enum_data.get("BasicSalary", {})}
# }
# logger.info(enum_mapping_kwargs)

# value_list_kwargs: Dict = {
#     "${GENDER}": list(learner_enum_data.get("Gender", {}).keys()),
#     "${RESIDENTIAL_STATUS}": list(learner_enum_data.get("ResidentialStatus", {}).keys()),
#     "${PASS_TYPE}": list(learner_enum_data.get("PassType", {}).keys()),
#     "${RACE}": [i.get("name") for i in learner_enum_data.get("Race", {})],
#     "${COUNTRY}": [i.get("name") for i in learner_enum_data.get("CountryCode", {})],
#     "${NATIONALITY}": [i.get("name") for i in learner_enum_data.get("NationalityCode", {})],
# }
# logger.info(value_list_kwargs)

# ========== Load processing config ==========
processing_config_data = read_yaml_file_strategy(processing_congfig_file_path).load()

df_processing_config = pd.DataFrame(processing_config_data)
df_processing_config = process_config(df_processing_config, common_kwargs)
df_processing_config.to_excel(f"{file_name}_processing_config.xlsx", index=False)


# ========== Load validation config ==========
validation_config = read_yaml_file_strategy(validation_config_file_path).load()

df = pd.DataFrame(validation_config)
# df = process_config(df, {**common_kwargs, **value_list_kwargs})
df = process_config(df, {**common_kwargs}) # internal suspension list
df.to_excel(f"{file_name}_validation_config.xlsx", index=False)
# logger.info(df["ref_info"].values.tolist())


# ========== Read data ==========
df_learner_account: pd.DataFrame = read_excel_file_strategy(data_file_path).load(sheet_name=sheet_name, dtype=str, engine="calamine")
# logger.info(df_learner_account.columns)

data_validation_pipeline(
    df=df_learner_account,
    df_processing_config=df_processing_config,
    df_validation_config=df,
    file_path=data_file_path,
    sheet_name=sheet_name
)

# # Internal suspended list
# from pipeline.setup import validation_strategy_factory, outer_reference_registry
# outer_validation_strategy = validation_strategy_factory.get_strategy("outer_reference")
# outer_ref_columns = df.loc[df["ref_info"].notna()]
# for index, config in outer_ref_columns.iterrows():
#     result = outer_validation_strategy(
#         df=df_learner_account,
#         factory=validation_strategy_factory,
#         outer_reference_registry=outer_reference_registry,
#         **config
#     ).run()


# run validation -> which can in validation_config.yaml -> common config
# Which special -> write special validation -> inherit ValidationStrategy
# # need do: special logic -> pipeline
# connect to slack, discord, emal, sms each running -> report



# ===========================

# remove white space
# remove_white_space_columns = df_processing_config.loc[df_processing_config["type"] == "remove_white_space"]
# remove_white_space_processing = processing_strategy_factory.get_strategy("remove_white_space")
# for index, config in remove_white_space_columns.iterrows():
#     df_learner_account = remove_white_space_processing(df=df_learner_account, **config).run()
    
# # lower case
# string_case_configs = df_processing_config.loc[df_processing_config["type"] == "string_case"]
# string_case_processing = processing_strategy_factory.get_strategy("string_case")
# for index, config in string_case_configs.iterrows():
#     df_learner_account = string_case_processing(df_learner_account, **config).run()

# # Fill default value: be carefull
# # 2 types:
# # no condition: can fill all
# # having condition: for example: Pass type justr fill default if Reasidental status is not Foreigner, if is Foreigner, it will be validation case: pass type can not empty if Reasidental status is Foreigner
# fill_default_configs = df_processing_config.loc[df_processing_config["type"] == "fill_default"]
# fill_default_processing = processing_strategy_factory.get_strategy("fill_default")(df_learner_account)
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
