import json
from typing import Optional, Union, Dict, List
from .base_strategy import ReadDataStrategy

class ReadJsonFileStrategy(ReadDataStrategy):

    def __init__(self, file_path: str = "") -> None:
        super().__init__(file_path)

    def load(self, *args, **kwargs) -> Optional[Union[Dict, List]]:
        with open(self.file_path, 'r', *args, **kwargs) as file:
            return json.load(file)
