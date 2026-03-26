from typing import Optional
from .logger_wrapper import logger_wrapper


@logger_wrapper
def detect_file_type(file_path: str = "") -> Optional[str]:
    """
    Detect file type for read/write file.
    """
    if not file_path:
        raise ValueError(f"[{detect_file_type.__name__}] file_path is required.")

    file_type: str = file_path.split(".")[-1]

    match file_type:
        case "yaml" | "yml":
            return "yaml"
        case "json":
            return "json"
        case "xlsx" | "xls":
            return "excel"
        case "parquet":
            return "parquet"
        case "csv":
            return "csv"
        case "feather":
            return "feather"
        case _:
            raise ValueError(f"[{detect_file_type.__name__}] Invalid file type: {file_type}.")