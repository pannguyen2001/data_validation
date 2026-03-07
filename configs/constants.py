import datetime
from dotenv import load_dotenv
import os

load_dotenv()
date_today = datetime.datetime.now().strftime("%Y-%m-%d")
datetime_today = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")

BASE_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
VALIDATION_FILE_PATH = os.getenv("VALIDATION_FILE_PATH")
log_file_path = os.path.join(BASE_PATH, "logs", f"{date_today}.log")
report_folder_path = os.path.join(BASE_PATH, "reports", date_today)
# if not os.path.exists(report_folder_path):
#     os.makedirs(report_folder_path)


