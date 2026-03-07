import sys
from loguru import logger
from string import Template
import os
from datetime import datetime
from configs.constants import log_file_path

os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y-%m-%d")

error_template = Template("""[${funct_name}] has error:
${error}""")

logger.remove()

logger.add(
    sys.stdout,
    colorize=True,
    format="<level>[{level}]</level>[<green>{time:YYYY-MM-DD HH:mm:ss}</green>][<cyan>{name}:{function}:{line}</cyan>]\n<level>{message}</level>",
    # level="TRACE" # default is DEBUG
)

logger.add(
    log_file_path,
    colorize=False,
    format="[{level}][{time:YYYY-MM-DD HH:mm:ss}][{name}:{function}:{line}]\n{message}"
)

logger.add(
    f"logs/error_logs/{timestamp}.log",
    level="ERROR",
    colorize=False,
    format="[{level}][{time:YYYY-MM-DD HH:mm:ss}][{name}:{function}:{line}]\n{message}"
)
