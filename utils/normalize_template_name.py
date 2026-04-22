# import re
# from pathlib import Path
# from .logger_wrapper import logger_wrapper


# @logger_wrapper
# def normalize_template_name(file_path: str, keep_glued_words: bool = False) -> str:
#     """
#     Convert file path like:
#       data/raw_data/Data Template 06_02_Course IntakeSetupAndOthers_SLP3P4.xlsx
#       /home/user/.../Data Template M02_Master Data Setup_C2R1.xlsx

#     Into:
#       COURSE_INTAKE_SETUP_AND_OTHERS
#       MASTER_DATA_SETUP

#     If keep_glued_words=True:
#       COURSE_INTAKE_SETUP_ANDOTHERS
#     """

#     if not file_path or not isinstance(file_path, str):
#         raise ValueError("file_path must be a non-empty string")

#     # 1) remove wrapping quotes + spaces
#     file_path = file_path.strip().strip('"').strip("'")

#     # 2) get filename only, remove extension
#     name = Path(file_path).stem

#     # 3) remove leading "Data Template" (case-insensitive)
#     name = re.sub(r"^\s*Data\s+Template\s*", "", name, flags=re.IGNORECASE)

#     # 4) remove leading codes like:
#     #    06_02_
#     #    M02_
#     #    123_
#     name = re.sub(r"^(?:[A-Z]?\d+(?:_\d+)*)_", "", name, flags=re.IGNORECASE)

#     # 5) remove trailing release/version codes like:
#     #    _C2R1
#     #    _SLP3P4
#     #    _V2
#     name = re.sub(r"_(?:[A-Z]+\d+(?:[A-Z]*\d*)*)$", "", name, flags=re.IGNORECASE)

#     # 6) split CamelCase inside words:
#     #    IntakeSetupAndOthers -> Intake Setup And Others
#     if not keep_glued_words:
#         name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)

#     # 7) normalize separators: spaces, hyphens -> underscore
#     name = re.sub(r"[\s\-]+", "_", name)

#     # 8) collapse multiple underscores
#     name = re.sub(r"_+", "_", name).strip("_")

#     # 9) uppercase
#     return name.upper()


import re
from .logger_wrapper import logger_wrapper

def transform_to_snake_case(text):
        # 1. Insert underscores before uppercase letters (CamelCase handling)
        # e.g., IntakeSetup -> Intake_Setup
        s1 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', text)
        # 2. Replace spaces with underscores and convert to uppercase
        return s1.replace(' ', '_').upper()


@logger_wrapper
def normalize_template_name(file_path: str) -> str:
    # Regex logic:
    # Look for 'Data Template ', skip the ID part (M06, 06_02, etc.),
    # then capture everything until the next underscore separator (_C2, _SLP) or .xlsx
    pattern = r"Data Template (?:[A-Za-z0-9]\d+|\d+_\d+)_+(.*?)(?=_C\d|_SLP|\.xlsx)"
    match = re.search(pattern, file_path)
    if match:
        raw_name = match.group(1).strip()
        # Clean and handle CamelCase
        clean_name = transform_to_snake_case(raw_name)
        # Remove any resulting double underscores
        return re.sub('_+', '_', clean_name)
    return None