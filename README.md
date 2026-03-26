# Data validation tool.

## I. Usage
- Load data from many file types: csv, json, excel, parquet, etc.
- Validate data against a schema.
- Generate a report with the validation results.
- Export the report to a file.

## II. Features
- Support for multiple data sources, formats.
- Processing data before/after validation for keeping data clean and union format:
    - Remove white space.
    - Transform string: uppercase, lowercase, trim, etc.
    - Split string by separator.
    - Enum mapping data.
    - Fill default value.
- Support for multiple validation rules.
    - Check required fields.
    - Check data type.
    - Check data format.
    - Check data length.
    - Check data range.
    - Check data uniqueness.
    - Check value consistency.
    - Check data relationship between columns.
    - Check data relationship between tables.
    - Check data consistency.
- Setup more custome rule logics.
- Support for exporting validation results to a file.
- Support logging.

## III. Technologies
- Python
- Pandas
- Loguru
- Pytest

## IV. Setup and config
1. Install virtual environment.
```
python -m venv .venv
```
2. Install requirements.
```
Active virtual environment: & "<absolute folder path>\.venv\Scripts\Activate.ps1"
pip install -r requirements.txt
```
3. Check .venv python environment
```
where python
echo $env:VIRTUAL_ENV
```

## V. Run project
1. Activate virtual environment
```
& <project_absolute_folder_path>\.venv\Scripts\Activate.ps1
```
2. Run project
```
python main.py
```
3. Run tests
```
pytest --alluredir=allure-results --clean-alluredir
allure generate allure-results -o allure-report
allure open ./allure-report
```

## VI. TODO
- [X] Add data type check. **
- [X] Add fill default value based on condition. **
- [X] Add outer reference validation. ***
- [X] Add data validation pipeline for many sheets and files.
- [ ] Add more unit tests.
- [ ] Add write report to file and database: csv, json, exce, parquet, SQLite, PostgreSQL, Mongodb.
- [X] Add cache memory.
- [X] Add concurency, multiple threads, multiple processings, async await.
- [ ] Add CI/CD.
- [ ] Improve perfomance: time running, complexity.
- [ ] Add function to check time excecition, profiling, benchmark.