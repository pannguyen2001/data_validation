import pandas as pd
from abc import ABC, abstractmethod
from utils.logger_wrapper import logger_wrapper
from utils import mark_result


class ValidationStrategy(ABC):
    def __init__(self, df: pd.DataFrame = None, *args, **kwargs) -> None:
        self.df = df
        self.args = args
        self.kwargs = kwargs

        if self.df is None:
            raise ValueError(
                f"[{self.__class__.__name__}] df and config must be provided."
            )
        if self.df.empty:
            raise ValueError(f"[{self.__class__.__name__}] df must not be empty.")

        if "validation_result" not in df.columns:
            self.df["validation_result"] = [set() for i in range(self.df.shape[0])]
        self.mask = pd.Series(index=self.df.index)

    def is_empty(self, data) -> bool:
        """Check if data is empty"""
        if data is None or data.empty:
            raise ValueError(f"[{self.__class__.__name__}] Data is empty.")
        return False

    @abstractmethod
    @logger_wrapper
    def validate(self, column: str) -> pd.Series:
        """Return vectorized validation result. True for invalid, False for valid."""
        raise NotImplementedError

    @logger_wrapper
    def run(self) -> None:
        column = self.kwargs.get("name")
        if not column:
            raise ValueError(f"[{self.__class__.__name__}] column is required.")
        set_columns = {column} if isinstance(column, str) else set(column)
        invalid_columns = set_columns - set(self.df.columns.values.tolist())
        if invalid_columns:
            raise ValueError(
                f"[{self.__class__.__name__}] Column '{invalid_columns}' not in df: {self.df.columns.values.tolist()}"
            )

        self.is_empty(self.df[column])
        self.mask = self.validate(column)

        if self.__class__.__name__ not in ["InnerReferenceValidation", "OuterReferenceValidation"]:
            validation_type = self.kwargs.get("validation_type")
            message = self.kwargs.get("message")
            mark_result(self.df, self.mask, column, validation_type, message)

        return self.mask

    # @logger_wrapper
    # def mark_result(
    #     self,
    #     column: str = "",
    #     *args,
    #     **kwargs,
    # ) -> None:
    #     error_amount: int = self.mask.sum()
    #     if error_amount == 0:
    #         logger.success(f"[{column}] [{self.validation_type}] All records are valid.")
    #         return

    #     validation_type = self.validation_type
    #     message = self.message or "Error"
    #     result_report = self.result_report.safe_substitute(column=column, validation_type=validation_type, message=message)

    #     if message != "Error":
    #         self.df.loc[self.mask, "validation_result"] = self.df.loc[
    #             self.mask, "validation_result"
    #         ].map(lambda x: x.union({result_report}))

    #         shape: int = self.mask.shape[0]
    #         sample_index_errors: List = (
    #             self.mask.where(self.mask).dropna().index + 2
    #         ).values.tolist()[:5]
    #         logger.error(
    #             f"{result_report}. {error_amount}/{shape} records are invalid. Excel index example: {sample_index_errors}."
    #         )
    #     return
