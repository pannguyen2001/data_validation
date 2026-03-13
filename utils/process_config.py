import re
import pandas as pd
from typing import Dict
from utils.logger_wrapper import logger_wrapper


# @logger_wrapper
# def replace_placeholders(data, mapping):
#     if isinstance(data, dict):
#         return {k: replace_placeholders(v, mapping) for k, v in data.items()}
#     elif isinstance(data, list):
#         return [replace_placeholders(i, mapping) for i in data]
#     elif isinstance(data, str) and data in mapping:
#         return mapping[data]
#     return data


pattern = re.compile(r"\$\{[A-Z0-9_]+\}")
# re.compile(r"\$\{[^}]+\}")

@logger_wrapper
def replace_placeholders(obj, mapping):
    if isinstance(obj, dict):
        return {k: replace_placeholders(v, mapping) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [replace_placeholders(v, mapping) for v in obj]

    elif isinstance(obj, tuple):
        return tuple(replace_placeholders(v, mapping) for v in obj)

    elif isinstance(obj, str):

        # case 1: whole string is placeholder
        if obj in mapping:
            return mapping[obj]

        # case 2: placeholder inside string
        def repl(match):
            key = match.group()
            value = mapping.get(key, key)
            return str(value)

        return pattern.sub(repl, obj)

    return obj

@logger_wrapper
def process_config(df: pd.DataFrame = None, value_mapping: Dict = None):
    """
    Process config:
        - Explode data.
        - Replace ${...} to real values.
    """
    if df is None or df.empty:
        raise ValueError(f"[{process_config.__name__}] df is required.")
    
    df = df.explode("columns", ignore_index=True)
    df = df.join(
        pd.DataFrame(
            df
            .pop("columns")
            .values
            .tolist()
        )
    )
    df = df.explode("rules", ignore_index=True)
    df = df.join(
        pd.DataFrame(
            df
            .pop('rules')
            .values
            .tolist()
        )
    )

    if value_mapping is not None:
        df = df.map(
            lambda x: value_mapping.get(x, x) if isinstance(x, str) else x
        )
        if "ref_info" in df.columns:
            not_na_mask: pd.Series = df["ref_info"].notna()
            df.loc[not_na_mask, "ref_info"] = df.loc[not_na_mask, "ref_info"].map(lambda x: replace_placeholders(x, value_mapping))

    return df