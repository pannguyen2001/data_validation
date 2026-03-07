from typing import Callable
from functools import wraps
import traceback
from loguru import logger

def logger_wrapper(func: Callable) -> Callable:
    @wraps(func)
    def wrap(*args, **kwargs):
        try:
            # logger.info(f"[{func.__name__}] Start execution.")
            # result = func(*args, **kwargs)
            # logger.success(f"[{func.__name__}] End execution.")
            # return result
            return func(*args, **kwargs)
        except Exception as e:
            tb = "".join(traceback.TracebackException.from_exception(e).format())
            logger.error(f"\x1b[0mError in {func.__name__}: {e}\n{tb}")
            return None

    return wrap