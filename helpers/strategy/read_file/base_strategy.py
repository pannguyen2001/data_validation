import os
from abc import ABC, abstractmethod
from typing import Any
from utils.logger import logger
from utils.logger_wrapper import logger_wrapper

class ReadDataStrategy(ABC):

    def __init__(self, file_path: str = "") -> None:
        self.file_path = file_path

        if not self.file_path:
            raise ValueError(f"[{self.__class__.__name__}] file_path is required.")
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"[{self.__class__.__name__}] File not found: {self.file_path}.")
    
    @abstractmethod
    @logger_wrapper
    def load(self, *args, **kwargs) -> Any:
        """
        Read data from a file and return its contents as a dictionary.
        """
        logger.info(f"Read data from file: {self.file_path}")
        raise NotImplementedError
