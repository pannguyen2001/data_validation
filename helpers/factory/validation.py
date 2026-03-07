from functools import partial
from typing import Dict, Optional
from helpers.strategy.validation import ValidationStrategy
from utils.logger_wrapper import logger_wrapper


class ValidationStrategyFactory:
    def __init__(self, context: Optional[Dict] = None):
        self.strategies: Dict[str, ValidationStrategy] = {}
        self.context = context

    @logger_wrapper
    def register(self, name: str, strategy: ValidationStrategy):
        self.strategies[name] = strategy

    @logger_wrapper
    def get_strategy(self, name: str, **kwargs) -> ValidationStrategy:
        if name not in self.strategies.keys():
            raise ValueError(f"[{self.__class__.__name__}] Unknown rule type: {name}.")
        return partial(self.strategies[name], **kwargs)

    @logger_wrapper
    def build_strategy(
        self, name: str, config: Optional[Dict] = None, *args, **kwargs
    ) -> ValidationStrategy:
        rule_type = kwargs.get("type")
        rule_class = self.strategies.get(rule_type)

        if rule_type not in list(self.strategies.keys()):
            raise ValueError(
                f"[{self.__class__.__name__}] Unknown rule type: {rule_type}."
            )

        if rule_class is None:
            raise ValueError(
                f"[{self.__class__.__name__}] Rule class not found for rule type: {rule_type}."
            )

        if self.context is None:
            self.context = {}

        return rule_class(*args, **kwargs, **self.context)
