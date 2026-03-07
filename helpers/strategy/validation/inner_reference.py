import pandas as pd
from typing import Optional
from helpers.strategy.validation.base_strategy import ValidationStrategy
from helpers.factory import ValidationStrategyFactory
from utils.condition_parser import ConditionParser
from utils import mark_result


class InnerReferenceValidation(ValidationStrategy):
    def __init__(
        self,
        df: pd.DataFrame = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(df, *args, **kwargs)
        self.ref_info = self.kwargs.get("ref_info")
        self.factory = self.kwargs.get("factory")
        self.kwargs["validation_type"] = "Check inner reference"
        self.compiled_refs = []

        for ref in self.ref_info:
            condition = ref.get("condition")
            sub_rules = [
                self.factory.build_strategy(
                    rule.get("type"),
                    df=self.df,
                    **rule,  # rule: {type:..., empty_list:..., datetime_format: ..., ....}
                )
                for rule in ref.get("ref_rules", [])
            ]
            self.compiled_refs.append(
                {"condition": condition, "rules": sub_rules}
            )  # recursion

    def validate(self, column: str) -> pd.Series:
        mask = pd.Series(False, index=self.df.index)

        for ref in self.compiled_refs:
            condition_mask = ConditionParser.build_mask(self.df, ref["condition"])

            for rule in ref["rules"]:
                sub_mask = rule.validate(column)
                # Apply only where condition is true
                invalid_mask = condition_mask & sub_mask
                mask = mask | invalid_mask
                mark_result(self.df, mask, column, self.kwargs["validation_type"], rule.kwargs["message"])

        return mask
