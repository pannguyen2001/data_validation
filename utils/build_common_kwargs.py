import pandas as pd
import numpy as np
import datetime
from configs.constants import DATA_FOLDER_PATH
from .normalize_template_name import normalize_template_name
from .logger_wrapper import logger_wrapper


@logger_wrapper
def build_common_kwargs(file_paths: list[str]) -> dict:
    common_kwargs = {
        "${DATA_FOLDER_PATH}": DATA_FOLDER_PATH,
        "${EMPTY_LIST}": ["", "nan", np.nan, pd.NA, None, "NA", "<NA>", "NAT", "null"],
        "now": datetime.datetime.now(),
    }

    for file_path in file_paths:
        key = normalize_template_name(file_path)
        common_kwargs[f"${{{key}_FILE_PATH}}"] = file_path

    return common_kwargs