from utils.logger import logger
from helpers.factory import (
    ValidationStrategyFactory,
    ReadFileStrategyFactory,
    ProcessingStrategyFactory,
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
    ValueListValidation,
    DataTypeValidation,
    InRangeNumberValidation,
    InRangeStringLengthValidation,
    DatetimeLogicValidation
    )
from helpers.strategy.processing import (
    RemoveWhiteSpaceProcessing,
    StringCaseProcessing,
    SplitStringProcessing,
    EnumMappingProcessing,
    FillDefaultValueProcessing,
    ConvertDataTypeProcessing
    )
from helpers.strategy.write_data import (
    WriteToExcelStrategy
)
from helpers.strategy.validation.inner_reference import InnerReferenceValidation
from helpers.strategy.validation.outer_reference import OuterReferenceValidation, OuterReferenceRegistry


logger.info("Setup strategy")

# ========== ReadFileStrategy ==========
read_file_strategy_factory = ReadFileStrategyFactory()
read_file_strategy_factory.register("excel", ReadExcelFileStrategy)
read_file_strategy_factory.register("json", ReadJsonFileStrategy)
read_file_strategy_factory.register("yaml", ReadYAMLFileStrategy)

# ========== PreprocessingStrategy ==========
processing_strategy_factory = ProcessingStrategyFactory()
processing_strategy_factory.register("remove_white_space", RemoveWhiteSpaceProcessing)
processing_strategy_factory.register("string_case", StringCaseProcessing)
processing_strategy_factory.register("split_string", SplitStringProcessing)
processing_strategy_factory.register("enum_mapping", EnumMappingProcessing)
processing_strategy_factory.register("fill_default", FillDefaultValueProcessing)
processing_strategy_factory.register("convert_data_type", ConvertDataTypeProcessing)

# ========== ValidationStrategy ==========
validation_strategy_factory = ValidationStrategyFactory()
validation_strategy_factory.register("mandatory", MandatoryValidation)
validation_strategy_factory.register("unique", UniqueValidation)
validation_strategy_factory.register("inner_reference", InnerReferenceValidation)
validation_strategy_factory.register("datetime_format", DatetimeFormatValidation)
validation_strategy_factory.register("datetime_range", InRangeDateTimeValidation)
validation_strategy_factory.register("value_list", ValueListValidation)
validation_strategy_factory.register("data_type", DataTypeValidation)
validation_strategy_factory.register("outer_reference", OuterReferenceValidation)
validation_strategy_factory.register("in_range_number", InRangeNumberValidation)
validation_strategy_factory.register("in_range_string_length", InRangeStringLengthValidation)
validation_strategy_factory.register("datetime_logic", DatetimeLogicValidation)
outer_reference_registry = OuterReferenceRegistry(read_file_strategy_factory)

# ========== WriteDataStrategy ==========
write_data_strategy_factory = WriteDataStrategyFactory()
write_data_strategy_factory.register("excel", WriteToExcelStrategy)


logger.success("Setup strategy complete.")
