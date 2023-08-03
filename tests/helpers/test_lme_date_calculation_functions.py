from prep.helpers import lme_date_calculation_functions
from upedata.static_data import Holiday

import pytest

from datetime import date, datetime
import logging


BASE_HOLIDAY_DATA = [
    ("2023-08-28", 1.0, True),
    ("2023-09-04", 1.0, False),
    ("2023-10-09", 1.0, False),
    ("2023-11-23", 1.0, False),
    ("2023-12-25", 1.0, True),
    ("2023-12-26", 1.0, True),
    ("2024-01-01", 1.0, True),
    ("2024-01-15", 1.0, False),
    ("2024-02-19", 1.0, False),
    ("2024-03-29", 1.0, True),
    ("2024-04-01", 1.0, True),
    ("2024-05-06", 1.0, True),
    ("2024-05-27", 1.0, True),
    ("2024-07-04", 1.0, False),
    ("2024-08-26", 1.0, True),
    ("2024-09-02", 1.0, False),
    ("2024-10-14", 1.0, False),
    ("2024-11-11", 1.0, False),
    ("2024-11-28", 1.0, False),
    ("2024-12-25", 1.0, True),
    ("2024-12-26", 1.0, True),
    ("2025-01-01", 1.0, True),
    ("2025-01-20", 1.0, False),
    ("2025-02-17", 1.0, False),
    ("2025-04-18", 1.0, True),
    ("2025-04-21", 1.0, True),
    ("2025-05-05", 1.0, True),
    ("2025-05-26", 1.0, True),
    ("2025-06-19", 1.0, False),
    ("2025-07-04", 1.0, False),
    ("2025-08-25", 1.0, True),
    ("2025-09-01", 1.0, False),
    ("2025-10-13", 1.0, False),
    ("2025-11-11", 1.0, False),
    ("2025-11-27", 1.0, False),
    ("2025-12-25", 1.0, True),
    ("2025-12-26", 1.0, True),
]

LME_2023_THROUGH_2024_NON_PROMPTS = [
    datetime.strptime(str_date, r"%Y-%m-%d").date()
    for str_date, _, _ in BASE_HOLIDAY_DATA
]

MOCK_HOLIDAYS = [
    Holiday(
        holiday_date=datetime.strptime(str_date, r"%Y-%m-%d").date(),
        holiday_weight=weight_data,
        is_closure_date=closure_data,
    )
    for str_date, weight_data, closure_data in BASE_HOLIDAY_DATA
]


@pytest.mark.parametrize(
    ["input_year", "expected_date"],
    [
        (2022, date(2022, 4, 15)),
        (2023, date(2023, 4, 7)),
        (2024, date(2024, 3, 29)),
        (2025, date(2025, 4, 18)),
    ],
)
def test_get_good_friday_date(input_year, expected_date):
    assert expected_date == lme_date_calculation_functions.get_good_friday_date(
        input_year
    )


@pytest.mark.parametrize(
    "test_base_datetime",
    [
        datetime(2023, 1, 1),
        datetime(2023, 6, 30),
        datetime(2023, 11, 30),
        datetime(2024, 4, 1),
    ],
)
def test_lme_prompt_map_has_no_circular_mappings(test_base_datetime):
    lme_prompt_map = lme_date_calculation_functions.get_lme_prompt_map(
        LME_2023_THROUGH_2024_NON_PROMPTS, test_base_datetime
    )

    # I can't think of a more elegant way to do this so brute force it is
    encountered_loop = False
    for key, value in lme_prompt_map.items():
        if key != value:
            date_in_key = key
            already_found_dates = []
            try:
                while date_in_key != (date_in_key := lme_prompt_map[date_in_key]):
                    if date_in_key in already_found_dates:
                        logging.error(
                            "Encountered loop of mapped dates starting at %s mapping to %s",
                            key,
                            value,
                        )
                        encountered_loop = True
                        break
                    already_found_dates.append(date_in_key)
            except KeyError:
                # This occurs at the end of the map, which can sometimes be open
                pass
    assert not encountered_loop


@pytest.mark.parametrize(
    "test_base_datetime",
    [
        datetime(2023, 1, 1),
        datetime(2023, 6, 30),
        datetime(2023, 11, 30),
        datetime(2024, 4, 1),
    ],
)
def test_lme_prompt_map_has_no_indirect_mappings(test_base_datetime):
    lme_prompt_map = lme_date_calculation_functions.get_lme_prompt_map(
        LME_2023_THROUGH_2024_NON_PROMPTS, _current_date=test_base_datetime
    )

    # I can't think of a more elegant way to do this so brute force it is
    encountered_indirect_mapping = False
    for key, value in lme_prompt_map.items():
        if key != value:
            date_in_key = key
            try:
                date_out_key = lme_prompt_map[date_in_key]
                if date_out_key != lme_prompt_map[date_out_key]:
                    encountered_indirect_mapping = True
            except KeyError:
                # This occurs at the end of the map, which can sometimes be open
                pass
    assert not encountered_indirect_mapping


@pytest.mark.parametrize(
    ["base_datetime", "expected_date"],
    [
        [datetime(2023, 11, 21, 12, 15), date(2023, 11, 24)],
        [datetime(2023, 11, 22, 15, 51), date(2023, 11, 24)],
        [datetime(2023, 11, 30, 15, 1), date(2023, 12, 4)],
        [datetime(2024, 3, 28, 13, 30), date(2024, 4, 3)],
        [datetime(2024, 12, 24, 13, 30), date(2024, 12, 30)],
        [datetime(2024, 12, 24, 19, 31), date(2024, 12, 31)],
    ],
)
def test_get_cash_date(base_datetime, expected_date):
    assert (
        lme_date_calculation_functions.get_cash_date(base_datetime, MOCK_HOLIDAYS)
        == expected_date
    )
