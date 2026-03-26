import pandas as pd
from typing import Optional
from utils.logger_wrapper import logger_wrapper
from utils.mark_result import mark_result
from utils.read_reference_data import read_reference_data

@logger_wrapper
def course_financial_setup_discount():

    # check id + course/mod in CourseFinancialSetupRevenue but not in CourseFinancialSetupDiscount
    # check id + course/mod in CourseFinancialSetupDiscount but not in CourseFinancialSetupRevenue
    # check id + dis CourseFeeSetupDiscount but not in CourseFinancialSetupDiscount
    # check id + dis in CourseFinancialSetupDiscount but not in CourseFeeSetupDiscount
    # check id + dis + course/mode is enough, if lack -> issue, extra -> issue

    # Amount: Discount rate type = Flat -> not have value: error
    # Percentage of fee: Discount rate type = Percentage -> not have value: error
    #

    pass