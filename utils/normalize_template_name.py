import re
from pathlib import Path
from .logger_wrapper import logger_wrapper


@logger_wrapper
def normalize_template_name(file_path: str, keep_glued_words: bool = False) -> str:
    """
    Convert file path like:
      data/raw_data/Data Template 06_02_Course IntakeSetupAndOthers_SLP3P4.xlsx
      /home/user/.../Data Template M02_Master Data Setup_C2R1.xlsx

    Into:
      COURSE_INTAKE_SETUP_AND_OTHERS
      MASTER_DATA_SETUP

    If keep_glued_words=True:
      COURSE_INTAKE_SETUP_ANDOTHERS
    """

    if not file_path or not isinstance(file_path, str):
        raise ValueError("file_path must be a non-empty string")

    # 1) remove wrapping quotes + spaces
    file_path = file_path.strip().strip('"').strip("'")

    # 2) get filename only, remove extension
    name = Path(file_path).stem

    # 3) remove leading "Data Template" (case-insensitive)
    name = re.sub(r"^\s*Data\s+Template\s*", "", name, flags=re.IGNORECASE)

    # 4) remove leading codes like:
    #    06_02_
    #    M02_
    #    123_
    name = re.sub(r"^(?:[A-Z]?\d+(?:_\d+)*)_", "", name, flags=re.IGNORECASE)

    # 5) remove trailing release/version codes like:
    #    _C2R1
    #    _SLP3P4
    #    _V2
    name = re.sub(r"_(?:[A-Z]+\d+(?:[A-Z]*\d*)*)$", "", name, flags=re.IGNORECASE)

    # 6) split CamelCase inside words:
    #    IntakeSetupAndOthers -> Intake Setup And Others
    if not keep_glued_words:
        name = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)

    # 7) normalize separators: spaces, hyphens -> underscore
    name = re.sub(r"[\s\-]+", "_", name)

    # 8) collapse multiple underscores
    name = re.sub(r"_+", "_", name).strip("_")

    # 9) uppercase
    return name.upper()