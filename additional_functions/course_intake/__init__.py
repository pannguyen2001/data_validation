from typing import Dict
from .course_intake_fee_setup import course_intake_fee_setup
from .course_intake_module_fee_rate import course_intake_module_fee_rate
from .course_intake_miscallenous_item import course_intake_miscallenous_item
from .course_intake_miscell_default_fee import course_intake_miscell_default_fee
from .course_intake_fee_setup_discount import course_intake_fee_setup_discount
from .course_intake_financial_revenue import course_intake_financial_revenue
from .course_intake_financial_discount import course_intake_financial_discount

course_intake_setup_additional_function: Dict = {
    "CourseIntakeFeeSetup": course_intake_fee_setup,
    "CourseIntakeModuleFeeRate": course_intake_module_fee_rate,
    "CourseIntakeMiscellaneousItem": course_intake_miscallenous_item,
    "CourseIntakeMiscellDefaultFee": course_intake_miscell_default_fee,
    "CourseIntakeFeeSetupDiscount": course_intake_fee_setup_discount,
    "CourseIntakeFinancialRevenue": course_intake_financial_revenue,
    "CourseIntakeFinancialDiscount": course_intake_financial_discount,
}
