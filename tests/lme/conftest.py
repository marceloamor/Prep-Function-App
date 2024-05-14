import pytest

from tests.lme.test_lme_date_calculation_functions import (
    BASE_HOLIDAY_DATA,
    LME_2023_THROUGH_2025_NON_PROMPTS,
    MOCK_HOLIDAYS,
)


@pytest.fixture()
def base_holiday_data():
    return BASE_HOLIDAY_DATA


@pytest.fixture()
def mock_holidays():
    return MOCK_HOLIDAYS


@pytest.fixture()
def lme_2023_through_2025_non_prompts():
    return LME_2023_THROUGH_2025_NON_PROMPTS
