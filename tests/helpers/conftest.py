from tests.helpers.test_lme_date_calculation_functions import (
    LME_2023_THROUGH_2025_NON_PROMPTS,
    BASE_HOLIDAY_DATA,
    MOCK_HOLIDAYS,
)

import pytest


@pytest.fixture()
def base_holiday_data():
    return BASE_HOLIDAY_DATA


@pytest.fixture()
def mock_holidays():
    return MOCK_HOLIDAYS


@pytest.fixture()
def lme_2023_through_2025_non_prompts():
    return LME_2023_THROUGH_2025_NON_PROMPTS
