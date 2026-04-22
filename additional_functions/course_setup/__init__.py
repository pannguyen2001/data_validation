from typing import Dict
from .course_fee_setup import course_fee_setup
from .course_fee_setup_miscellaneous_item import course_fee_setup_miscellaneous_item
from .course_fee_setup_discount import course_fee_setup_discount
from .course_financial_setup_revenue import course_financial_setup_revenue
from .course_module_fee_rate import course_module_fee_rate
from .course_financial_setup_miscell import course_financial_setup_miscell
from .course_financial_setup_discount import course_financial_setup_discount

course_setup_additional_function: Dict = {
    "CourseFeeSetup": course_fee_setup,
    "CourseFeeSetupMiscellaneousItem": course_fee_setup_miscellaneous_item,
    "CourseFeeSetupDiscount": course_fee_setup_discount,
    "CourseFinancialSetupRevenue": course_financial_setup_revenue,
    "CourseModuleFeeRate": course_module_fee_rate,
    "CourseFinancialSetupMiscell": course_financial_setup_miscell,
    "CourseFinancialSetupDiscount": course_financial_setup_discount
}

