import datetime
import os
from string import Template
from dotenv import load_dotenv

load_dotenv()
DATA_FOLDER_PATH = os.getenv("DATA_FOLDER_PATH")
CONSTANT_CONFIG_FOLDER_PATH = os.getenv("CONSTANT_CONFIG_FOLDER_PATH")
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
additional_report_folder_path = os.path.join(report_folder_path, datetime_today)
if not os.path.exists(additional_report_folder_path):
    os.makedirs(additional_report_folder_path)


default_success_message_values = {'column_name': 'a_default', 'extra_message': ''}
success_message: str = Template("[${column_name}] [Special logic] All reocords are valid. ${extra_message}")

