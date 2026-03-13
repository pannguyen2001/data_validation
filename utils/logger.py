import datetime
import zoneinfo
import sys
from loguru import logger
from string import Template
from configs.constants import log_file_path, date_today


def set_datetime(record):
    dt = datetime.datetime.strftime(datetime.datetime.now(tz=zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")), format="%Y-%m-%d %H:%M:%S")
    record["extra"]["datetime"] = dt
# loguru get local timezone: https://stackoverflow.com/questions/77826725/how-to-set-time-zone-in-loguru
# https://en.wikipedia.org/wiki/List_of_tz_database_time_zones

error_template = Template("""[${funct_name}] has error:
${error}""")

logger.remove()
logger.configure(patcher=set_datetime)
logger.add(
    sys.stdout,
    colorize=True,
    format="<level>[{level}]</level>[<green>{extra[datetime]}</green>][<cyan>{name}:{function}:{line}</cyan>]\n<level>{message}</level>",
    # level="TRACE" # default is DEBUG
)

logger.add(
    f"{log_file_path}/{date_today}.log",
    colorize=False,
    # format="[{level}][{time:YYYY-MM-DD HH:mm:ss}][{name}:{function}:{line}]\n{message}"
    format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}"
)

logger.add(
    f"logs/error_logs/{date_today}.log",
    level="ERROR",
    colorize=False,
    format="[{level}][{extra[datetime]}][{name}:{function}:{line}]\n{message}"
)
