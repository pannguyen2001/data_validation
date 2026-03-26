import operator
import pandas as pd
# from loguru import logger
from typing import Union
from utils.logger_wrapper import logger_wrapper

class ConditionParser:

    OPERATOR_MAP = {
        "==": operator.eq,
        "!=": operator.ne,
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "in": lambda x, y: x.isin(y),
        "not in": lambda x, y: ~x.isin(y),
        # "contain": lambda x, y: x.str.contains(y, regex=False, na=False),
        "contain": lambda x, y: all(x.str.extract(fr"\b{y}\b")),
        # "not contain": lambda x, y: ~x.str.contains(y, regex=False, na=False)
        "not contain": lambda x, y: ~all(x.str.extract(fr"\b{y}\b")),
    }

    LOGICAL_MAP = {
        "AND": lambda a, b: a & b,
        "OR": lambda a, b: a | b,
        "XOR": lambda a, b: a ^ b,
        "NOT": lambda a: ~a,
        "NOR": lambda a, b: ~(a | b),
        "NAND": lambda a, b: ~(a & b),
        "XNOR": lambda a, b: ~(a ^ b),
    }

    @classmethod
    @logger_wrapper
    def build_mask(cls, df: Union[pd.DataFrame, pd.Series], condition_config: dict) -> pd.Series:
        """
        Ex:
        {
          "operator": "AND",
          "conditions": [
            {
              "column": "Debtor type",
              "operator": "==",
              "values": "Individual"
            },
            {
              "column": "Status",
              "operator": "==",
              "values": "Active"
            }
          ],
        """

        if "conditions" in condition_config:
            # logger.info(condition_config)
            # Logical group
            operator_key = condition_config["operator"]
            conditions = condition_config["conditions"]

            masks = [cls.build_mask(df, c) for c in conditions]
            # build mask, meaning check each condition, return True or False each cell, mask[0] is the first condition, mask[1] is the second condition or result of group condition,... It mask from down to up. For example, mask[0] return True/False of Debtor type == Individual, mask[1] return True/False of Status == Active, then apply operator AND to get final result. If it has deeper, ex: (Debtor type == Individual) AND ((Status == Active) OR (Debtor type == Company)), mask[0] is top condition, mask[1] is result of ((Status == Active) OR (Debtor type == Company)). Final result is mask[0] & mask[1].

            result = masks[0]
            for mask in masks[1:]:
                result = cls.LOGICAL_MAP[operator_key](result, mask)

            return result

        else:
            # Leaf condition
            # logger.info(condition_config)
            # logger.info(df.columns)
            column = condition_config["column"]
            op = condition_config["operator"]
            values = condition_config["values"]

            if op not in cls.OPERATOR_MAP:
                raise ValueError(f"Unsupported operator: {op}")

            return cls.OPERATOR_MAP[op](df[column], values)