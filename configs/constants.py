import datetime
from dotenv import load_dotenv
import os


load_dotenv()
DATA_FOLDER_PATH = os.getenv("DATA_FOLDER_PATH")
CONSTANTS_CONFIG_FOLDER_PATH = os.getenv("CONSTANTS_CONFIG_FOLDER_PATH")
PROCESSING_CONFIG_FOLDER_PATH = os.getenv("PROCESSING_CONFIG_FOLDER_PATH")
VALIDATION_CONFIG_FOLDER_PATH = os.getenv("VALIDATION_CONFIG_FOLDER_PATH")

date_today = datetime.datetime.strftime(
    datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(hours=7))
    ),
    format="%Y-%m-%d"
)
time_today = datetime.datetime.strftime(
    datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(hours=7))
    ),
    format="%H-%M-%S"
)
datetime_today = datetime.datetime.strftime(
    datetime.datetime.now(
        tz=datetime.timezone(datetime.timedelta(hours=7))
    ),
    format="%Y-%m-%d %H-%M-%S"
)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
VALIDATION_FILE_PATH = os.getenv("VALIDATION_FILE_PATH")
log_file_path = os.path.join(BASE_PATH, "logs")
# if not os.path.exists(log_file_path):
#     os.makedirs(log_file_path)
report_folder_path = os.path.join(BASE_PATH, "reports", date_today)
if not os.path.exists(report_folder_path):
    os.makedirs(report_folder_path)


