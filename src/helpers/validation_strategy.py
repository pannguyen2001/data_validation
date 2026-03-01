from abc import ABC, abstractmethod
from typing import Dict, Optional
import pandas as pd

class ValidationStrategy(ABC):

    def __init__(
        self,
        config: Optional[Dict] = None
        ) -> None:
        self.config = config or {}

    @abstractmethod
    def validate(
        self,
        df: pd.DataFrame,
        column: str,
        *args,
        **kwargs
        ) -> pd.Series:
        '''Return vectorized validation result. True for invalid, False for valid.'''
        pass
