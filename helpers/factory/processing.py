from typing import Dict
from helpers.strategy.processing.base_strategy import PreprocessingStrategy
from utils.logger_wrapper import logger_wrapper

class ProcessingStrategyFactory:
    def __init__(self):
        self.strategies: Dict[str, PreprocessingStrategy] = {}

    @logger_wrapper
    def register(self, name: str, strategy: PreprocessingStrategy):
        self.strategies[name] = strategy

    @logger_wrapper
    def get_strategy(self, name: str) -> PreprocessingStrategy:
        if name not in self.strategies:
            raise ValueError(f"[{self.__class__.__name__}] Unknown rule type: {name}.")
        return self.strategies[name]



