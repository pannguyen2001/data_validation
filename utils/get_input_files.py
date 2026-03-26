from pathlib import Path
from .logger_wrapper import logger_wrapper

@logger_wrapper
def get_input_files(data_folder_path: str) -> list[str]:
    path = Path(data_folder_path)
    if not path.exists():
        raise FileNotFoundError(f"Path {path} does not exist.")

    return [str(p) for p in path.iterdir() if p.is_file()]