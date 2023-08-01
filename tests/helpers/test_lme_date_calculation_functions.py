from prep.helpers import lme_date_calculation_functions

import pytest

from datetime import date, datetime
import logging


LME_2023_THROUGH_2024_NON_PROMPTS = [
    datetime.strptime(str_date, r"%Y-%m-%d").date()
    for str_date in [
        "2023-05-29",
        "2023-06-19",
        "2023-07-04",
        "2023-08-28",
        "2023-09-04",
        "2023-10-09",
        "2023-11-23",
        "2023-12-25",
        "2023-12-26",
        "2024-01-01",
        "2024-01-15",
        "2024-02-19",
        "2024-03-29",
        "2024-04-01",
        "2024-05-06",
        "2024-05-27",
        "2024-07-04",
        "2024-08-26",
        "2024-09-02",
        "2024-10-14",
        "2024-11-11",
        "2024-11-28",
        "2024-12-25",
        "2024-12-26",
    ]
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
