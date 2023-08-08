from prep.helpers import lme_staticdata_utils, date_name_utilities
from upedata.static_data import Product, Future

import pytest

from datetime import datetime, date
from typing import List
import logging


@pytest.mark.parametrize(
    ["prompt_dates", "product", "expected_display_names", "expected_multiplier"],
    [
        [
            [datetime(2023, 11, 8)],
            Product(
                symbol="xlme-lcu-usd",
                short_name="lcu",
            ),
            ["LCU 2023-11-08"],
            25,
        ]
    ],
)
def test_generate_staticdata_lme_futures_from_prompts(
    prompt_dates: List[datetime],
    product: Product,
    expected_display_names: List[str],
    expected_multiplier: int,
):
    staticdata_futures = lme_staticdata_utils.gen_lme_futures(prompt_dates, product)
    for sd_future, expected_d_name in zip(staticdata_futures, expected_display_names):
        assert sd_future.display_name == expected_d_name, "Display name mismatch"
        assert sd_future.multiplier == expected_multiplier, "Multiplier mismatch"
