import os
import pandas as pd
import sys
import time
import socket
import platform
import psutil
import multiprocessing as mp
from typing import Any, Dict
from utils.logger import logger
from concurrent.futures import ProcessPoolExecutor, as_completed
from pipeline.validate_files import validate_files
from configs.constants import DATA_FOLDER_PATH
from utils.logger_wrapper import logger_wrapper
from utils.load_all_configs import load_all_configs
from utils.build_common_kwargs import build_common_kwargs
from utils.get_input_files import get_input_files
from utils.build_validation_sheet_set import build_validation_sheet_set
from additional_functions.course_setup import course_setup_additional_function
from additional_functions.course_intake import course_intake_setup_additional_function
from additional_functions.finance_process import finance_process_additional_function
import warnings
import polars as pl

# Ignore all warnings globally
warnings.filterwarnings("ignore")
# Ignore all PolarsWarnings
warnings.filterwarnings("ignore", category=pl.exceptions.PolarsWarning)

addtional_function: Dict = {
    **course_setup_additional_function,
    **course_intake_setup_additional_function,
    **finance_process_additional_function
    }


@logger_wrapper
def _safe_get_total_memory() -> Dict[str, Any]:
    """
    Try to get memory info.
    psutil is best if installed; otherwise return partial info.
    """
    try:
        import psutil
        vm = psutil.virtual_memory()
        return {
            "total_ram_gb": round(vm.total // 10**9, 2),
            "available_gb": round(vm.available // 10**9, 2),
            "used_ram_gb": round(vm.used // 10**9, 2),
            "ram_percent": vm.percent,
            "psutil_available": True,
        }
    except Exception:
        return {
            "total_ram_gb": None,
            "available_ram_gb": None,
            "used_ram_gb": None,
            "ram_percent": None,
            "psutil_available": False,
        }

@logger_wrapper
def _safe_get_physical_cpu_count() -> int | None:
    """
    Try to get physical CPU count if psutil exists.
    """
    try:
        import psutil
        return psutil.cpu_count(logical=False)
    except Exception:
        return None

@logger_wrapper
def get_env_info() -> Dict[str, Any]:
    """
    Collect environment/runtime info before starting validation.
    """
    info: Dict[str, Any] = {
        "run_started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "hostname": socket.gethostname(),
        "cwd": os.getcwd(),
        "python_version": sys.version.replace("\n", " "),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_logical_count": os.cpu_count(),
        "cpu_physical_count": _safe_get_physical_cpu_count(),
        "multiprocessing_start_method": mp.get_start_method(allow_none=True),
    }

    info.update(_safe_get_total_memory())
    return info


@logger_wrapper
def main(data_folder_path: str) -> None:
    if not data_folder_path:
        raise ValueError("data_folder_path is required.")
        return

    file_paths = get_input_files(data_folder_path)

    if not file_paths:
        logger.warning("No input files found.")
        return

    common_kwargs = build_common_kwargs(file_paths)
    df_processing, df_validation = load_all_configs(common_kwargs)
    validation_sheet_set = build_validation_sheet_set(df_validation)

    if not validation_sheet_set:
        logger.warning("No validation sheets configured.")
        return

    # max_workers = 1
    # max(1, min(os.cpu_count() or 1, len(file_paths)))

    results: list[dict[str]] = []

    # with ProcessPoolExecutor(max_workers=max_workers) as executor: # create multiple worker processes.
    #     futures = [
    #         executor.submit( # send one file as one task to a worker process.
    #             validate_files,
    #             file_path,
    #             df_processing,
    #             df_validation,
    #             validation_sheet_set,
    #         )
    #         for file_path in file_paths
    #     ]

    #     for future in as_completed(futures): # collect results in completion order, not submission order.
    #         try:
    #             result = future.result() # get returned data from worker (or raise uncaught worker exception).
    #             results.append(result)
    #             logger.info(
    #                 f"Completed file: {result['file_path']}. Validated: {len(result['validated_sheets'])}. Errors: {len(result['errors'])}"
    #             )
    #         except Exception as e:
    #             logger.exception(f"Worker future failed: {e}")
    for file_path in file_paths:
        # if "Course" not in file_path:
        #     continue
        try:
            result = validate_files(
                file_path,
                df_processing,
                df_validation,
                validation_sheet_set,
                common_kwargs=common_kwargs,
                additional_function=addtional_function
            )
            results.append(result)
        except Exception as e:
            logger.exception(f"Failed: {file_path}: {e}")

    logger.info(f"Done. Processed {len(results)} files.")
    # for item in results:
    #     logger.info(item)
    # import json
    # logger.info(json.dumps(results, indent=1))
    # logger.info(f"{'='*50}\n")


@logger_wrapper
def log_env_info(env_info: dict):
    df = pd.DataFrame([{"Key": k, "Value": str(v)} for k, v in env_info.items()])
    df.index = df.index + 1
    logger.info("========== ENVIRONMENT INFO ==========")
    # logger.info(tabulate(df, headers='keys', tablefmt='psql'))
    logger.info(df.to_markdown())
    logger.info("====================================")


def safe_worker_count(file_paths: list, safety_factor: float = 0.6) -> int:
    available_gb = psutil.virtual_memory().available / 1e9
    # Rough estimate: 1 GB per worker for large Excel files
    # Adjust estimated_gb_per_worker based on your actual file sizes
    estimated_gb_per_worker = 1.5
    memory_based = max(1, int((available_gb * safety_factor) / estimated_gb_per_worker))
    cpu_based = max(1, os.cpu_count() - 1)  # leave 1 core for the main process
    return min(memory_based, cpu_based, len(file_paths))


if __name__ == "__main__":
    mp.freeze_support()
    import psutil
    vm = psutil.virtual_memory()
    logger.info(f"Available RAM: {vm.available / 1e9:.1f} GB ({vm.percent}% used)")
    logger.info(safe_worker_count(file_paths=DATA_FOLDER_PATH))
    # env_info = get_env_info()
    # log_env_info(env_info)
    main(DATA_FOLDER_PATH)


# how to avoid memery leak

# from pipeline.setup import read_file_strategy_factory
# from .detect_file_type import detect_file_type
# from .get_input_files import get_input_files
# from typing import List, Dict, Optional
# from configs.constants import CONSTANT_CONFIG_FOLDER_PATH
# @logger_wrapper
# def load_constant(file_path: str = CONSTANT_CONFIG_FOLDER_PATH) -> Optional[Dict]:
#     constant_files: List = get_input_files(CONSTANT_CONFIG_FOLDER_PATH)
#     constant_data: Dict = {}
#     for file in constant_files:
#         file_type: str = detect_file_type(file)
#         temp_constant_data: Dict = read_file_strategy_factory.get_strategy(file_type)(file).load()
#         constant_data = {**constant_data, **temp_constant_data}
#     constant_data = {key: [item_key for item_key in value.keys()] for key, value in constant_data.items()}
#     logger.info(constant_data)
#     return constant_data




