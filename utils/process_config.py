import pandas as pd
from typing import Dict
from utils.logger_wrapper import logger_wrapper


@logger_wrapper
def replace_placeholders(data, mapping):
    if isinstance(data, dict):
        return {k: replace_placeholders(v, mapping) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_placeholders(i, mapping) for i in data]
    elif isinstance(data, str) and data in mapping:
        return mapping[data]
    return data

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