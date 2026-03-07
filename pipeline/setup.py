from helpers.factory import (
    ValidationStrategyFactory,
    ReadFileStrategyFactory,
    PreprocessingStrategyFactory,
    WriteDataStrategyFactory
    )
from helpers.strategy.read_file import (
    ReadExcelFileStrategy,
    ReadJsonFileStrategy,
    ReadYAMLFileStrategy
)
from helpers.strategy.validation import (
    MandatoryValidation,
    UniqueValidation,
    DatetimeFormatValidation,
    InRangeDateTimeValidation,
    ValueListValidation
    )
from helpers.strategy.preprocessing import (
    RemoveWhiteSpaceProcessing,
    StringCaseProcessing,
    SplitStringProcessing,
    EnumMappingProcessing,
    FillDefaultValueProcessing
    )
from helpers.strategy.write_data import (
    WriteToExcelStrategy
)
from helpers.strategy.validation.inner_reference import InnerReferenceValidation
from loguru import logger


logger.info("Setup strategy")

# ========== ReadFileStrategy ==========
read_file_strategy_factory = ReadFileStrategyFactory()
read_file_strategy_factory.register("excel", ReadExcelFileStrategy)
read_file_strategy_factory.register("json", ReadJsonFileStrategy)
read_file_strategy_factory.register("yaml", ReadYAMLFileStrategy)

# ========== PreprocessingStrategy ==========
preprocessing_strategy_factory = PreprocessingStrategyFactory()
preprocessing_strategy_factory.register("remove_white_space", RemoveWhiteSpaceProcessing)
preprocessing_strategy_factory.register("string_case", StringCaseProcessing)
preprocessing_strategy_factory.register("split_string", SplitStringProcessing)
preprocessing_strategy_factory.register("enum_mapping", EnumMappingProcessing)
preprocessing_strategy_factory.register("fill_default", FillDefaultValueProcessing)

# ========== ValidationStrategy ==========
validation_strategy_factory = ValidationStrategyFactory()
validation_strategy_factory.register("mandatory", MandatoryValidation)
validation_strategy_factory.register("unique", UniqueValidation)
validation_strategy_factory.register("inner_reference", InnerReferenceValidation)
validation_strategy_factory.register("datetime_format", DatetimeFormatValidation)
validation_strategy_factory.register("datetime_range", InRangeDateTimeValidation)
validation_strategy_factory.register("value_list", ValueListValidation)
# validation_strategy_factory.register("data_type", ValueListValidation)
# validation_strategy_factory.register("outer_reference", ValueListValidation)

# ========== WriteDataStrategy ==========
write_data_strategy_factory = WriteDataStrategyFactory()
write_data_strategy_factory.register("excel", WriteToExcelStrategy)


logger.success("Setup strategy complete.")