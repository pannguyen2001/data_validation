from pathlib import Path
from typing import Dict, Any, List
from loguru import logger
from utils.logger_wrapper import logger_wrapper
from utils.detect_file_type import detect_file_type
from pipeline.setup import read_file_strategy_factory

"""
Read and store all constant, processing, validation config:
{
    "constant": {
        "sheet_name" : {
        }
    },
    "processing" : {
        "sheet_name" : {
        }
    },
    "validation" : {
        "sheet_name" : {
        }
    }
}
"""

class ConfigCache:
    def __init__(self):
        # Initialize with separate dictionary objects
        self._cache: Dict[str, List[str, Any]] = {
            "constant": [],
            "processing": [],
            "validation": []
        }

    @logger_wrapper
    def load_configs(self, folder_path: str = "", config_type: str = ""):
        if config_type not in self._cache.keys():
            raise ValueError(f"Invalid type: {config_type}. Value must be in: {self._cache.keys()}")
        
        path = Path(folder_path)
        if not path.exists():
            raise FileNotFoundError(f"Path {folder_path} does not exist.")

        for file_path in path.iterdir():
            file_path = str(file_path)
            file_type: str = detect_file_type(file_path)
            data_config = read_file_strategy_factory.get_strategy(file_type)(file_path).load()
            # Stores the config indexed by sheet_name
            self._cache[config_type].extend(data_config)
        
        return self._cache

    @logger_wrapper
    def get(self, config_type: str = ""):
        """Get data storaged in cache."""
        if not config_type:
            return self._cache

        if config_type not in self._cache.keys():
            raise ValueError(f"Invalid type: {config_type}")
        
        return self._cache[config_type]


    @logger_wrapper
    def clear(self):
        """Explicitly clear memory after validation is complete"""
        self._cache["constant"].clear()
        self._cache["processing"].clear()
        logger.info("Cache cleared.")
