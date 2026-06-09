# import datetime
# import zoneinfo
# import sys
# from pathlib import Path
# from loguru import logger
# from string import Template
# from configs.constants import log_file_path, error_log_folder_path, date_today


# def set_datetime(record):
#     dt = datetime.datetime.strftime(datetime.datetime.now(tz=zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")), format="%Y-%m-%d %H:%M:%S")
#     record["extra"]["datetime"] = dt
# # loguru get local timezone: https://stackoverflow.com/questions/77826725/how-to-set-time-zone-in-loguru
# # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

# error_template = Template("""[${funct_name}] has error:
# ${error}""")

# error_log_file_path: str = f"{error_log_folder_path}/{date_today}.log"
# file_path = Path(error_log_file_path)
# file_path.unlink(missing_ok=True)
# with open(error_log_file_path, "w"):
#     pass


# logger.remove()
# logger.configure(patcher=set_datetime)
# logger.add(
#     sys.stdout,
#     colorize=True,
#     format="<level>[{level}]</level>[<green>{extra[datetime]}</green>][<cyan>{name}:{function}:{line}</cyan>]\n<level>{message}</level>",
#     # level="TRACE" # default is DEBUG
# )

# logger.add(
#     f"{log_file_path}/{date_today}.log",
#     colorize=False,
#     # format="[{level}][{time:YYYY-MM-DD HH:mm:ss}][{name}:{function}:{line}]\n{message}"
#     format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}"
# )

# logger.add(
#     error_log_file_path,
#     level="ERROR",
#     colorize=False,
#     format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}"
# )



import datetime
import sys
import zoneinfo
from pathlib import Path
from string import Template

from loguru import logger

from configs.constants import date_today, error_log_folder_path, log_file_path


def set_datetime(record):
    dt = datetime.datetime.strftime(
        datetime.datetime.now(tz=zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")),
        format="%Y-%m-%d %H:%M:%S",
    )
    record["extra"]["datetime"] = dt
    # loguru get local timezone: https://stackoverflow.com/questions/77826725/how-to-set-time-zone-in-loguru
    # https://en.wikipedia.org/wiki/List_of_tz_database_time_zones


error_template = Template("""[${funct_name}] has error:\n${error}""")
error_log_file_path: str = f"{error_log_folder_path}/{date_today}.log"


def setup_main_logger():
    """
    Call ONCE in the main process only.
    Sets up stdout + file sinks.
    """
    file_path = Path(error_log_file_path)
    file_path.unlink(missing_ok=True)
    with open(error_log_file_path, "w"):
        pass

    logger.remove()
    logger.configure(patcher=set_datetime)

    logger.add(
        sys.stdout,
        colorize=True,
        format="<level>[{level}]</level>[<green>{extra[datetime]}</green>][<cyan>{name}:{function}:{line}</cyan>]\n<level>{message}</level>",
    )
    logger.add(
        f"{log_file_path}/{date_today}.log",
        colorize=False,
        format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}",
        enqueue=True,  # ← loguru's built-in thread-safe async queue
    )
    logger.add(
        error_log_file_path,
        level="ERROR",
        colorize=False,
        format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}",
        enqueue=True,  # ← same here
    )


def setup_worker_logger():
    """
    Call in each worker process via ProcessPoolExecutor initializer.
    Workers only log to stdout — no file sinks.
    """
    logger.remove()
    logger.configure(patcher=set_datetime)
    logger.add(
        sys.stdout,
        colorize=True,
        format="<level>[{level}]</level>[<green>{extra[datetime]}</green>][<cyan>{name}:{function}:{line}</cyan>]\n<level>{message}</level>",
    )
    logger.add(
        f"{log_file_path}/{date_today}.log",
        colorize=False,
        format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}",
        enqueue=True,  # ← loguru's built-in thread-safe async queue
    )
    logger.add(
        error_log_file_path,
        level="ERROR",
        colorize=False,
        format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}",
        enqueue=True,  # ← same here
    )


# ← NO setup code at module level anymore!
# Import this file safely from anywhere without side effects.
