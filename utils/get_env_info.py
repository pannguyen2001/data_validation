import os
import sys
import time
import socket
import platform
import multiprocessing as mp
from typing import Any, Dict
from .logger_wrapper import logger_wrapper

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