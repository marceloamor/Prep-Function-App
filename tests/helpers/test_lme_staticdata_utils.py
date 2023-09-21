from prep.helpers import lme_staticdata_utils, date_name_utilities
from upedata.static_data import Product, Future, Option
from upedata.dynamic_data import VolSurface
from upedata import enums as upe_enums

import pytest

from datetime import datetime, date
from dateutil import relativedelta
from typing import List, Dict
from zoneinfo import ZoneInfo
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
        ],
        [
            [datetime(2023, 11, 8)],
            Product(
                symbol="xlme-lad-usd",
                short_name="lad",
            ),
            ["LAD 2023-11-08"],
            25,
        ],
        [
            [datetime(2023, 11, 8)],
            Product(
                symbol="xlme-lzh-usd",
                short_name="lzh",
            ),
            ["LZH 2023-11-08"],
            25,
        ],
        [
            [datetime(2024, 6, 8)],
            Product(
                symbol="xlme-pbd-usd",
                short_name="pbd",
            ),
            ["PBD 2024-06-08"],
            25,
        ],
        [
            [datetime(2025, 11, 8)],
            Product(
                symbol="xlme-lnd-usd",
                short_name="lnd",
            ),
            ["LND 2025-11-08"],
            6,
        ],
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


@pytest.mark.parametrize(
    ["input_futures_list", "option_template_data", "expected_options_list"],
    [
        [
            [
                Future(
                    symbol="xlme-lad-usd f 23-11-15",
                    display_name="LAD 2023-11-15",
                    product=Product(symbol="xlme-lad-usd", short_name="lad"),
                    expiry=datetime(2023, 11, 15, 19, 30),
                ),
                Future(
                    symbol="xlme-lad-usd f 23-11-14",
                    display_name="LAD 2023-11-14",
                    product=Product(symbol="xlme-lad-usd", short_name="lad"),
                    expiry=datetime(2023, 11, 14, 19, 30),
                ),
            ],
            {
                "shared": {
                    "time_type": 1,
                    "vol_type": 1,
                    "display_name": "#{<option>.product.short_name}£o#{map_month_year_coded(<option>.underlying_future.expiry)}£ @{strike}£ @{call_or_put}£",
                },
                "specific": {
                    "xlme-lad-usd": {
                        "vol_surface": {
                            "model_type": "delta_spline_wing",
                            "params": {
                                "50 Delta": 0.2,
                                "+25 DIFF": 0.01,
                                "-25 DIFF": 0.01,
                                "+10 DIFF": 0.02,
                                "-10 DIFF": 0.02,
                            },
                        },
                        "strike_intervals": [[1200, 25], [5000, -1]],
                        "multiplier": 25,
                    }
                },
            },
            [
                Option(
                    display_name=r"ladox3 @{strike}£ @{call_or_put}£",
                    symbol="xlme-lad-usd o 23-11-01 a",
                    multiplier=25,
                    strike_intervals=[[1200, 25], [5000, -1]],
                    expiry=datetime(
                        2023, 11, 1, 11, 15, tzinfo=ZoneInfo("Europe/London")
                    ),
                    vol_surface=VolSurface(
                        model_type="delta_spline_wing",
                        expiry=datetime(
                            2023, 11, 1, 11, 15, tzinfo=ZoneInfo("Europe/London")
                        ),
                        params={
                            "50 Delta": 0.2,
                            "+25 DIFF": 0.01,
                            "-25 DIFF": 0.01,
                            "+10 DIFF": 0.02,
                            "-10 DIFF": 0.02,
                        },
                    ),
                    vol_type=upe_enums.VolType.STANDARD,
                    time_type=upe_enums.TimeType.FULL_YEAR,
                )
            ],
        ]
    ],
)
def test_generate_staticdata_lme_options_from_futures(
    input_futures_list: List[Future],
    option_template_data: Dict,
    expected_options_list: List[Option],
):
    options_list = lme_staticdata_utils.gen_lme_options(
        input_futures_list, option_template_data
    )

    assert len(options_list) == len(
        expected_options_list
    ), "Unexpected number of options generated"
    for gen_option, expec_option in zip(options_list, expected_options_list):
        logging.warning(gen_option.symbol)
        assert (
            gen_option.display_name == expec_option.display_name
        ), "Display name mismatch"
        assert gen_option.symbol == expec_option.symbol, "Symbol mismatch"
        assert gen_option.multiplier == expec_option.multiplier, "Multiplier mismatch"
        assert (
            gen_option.strike_intervals == expec_option.strike_intervals
        ), "Strike intervals mismatch"
        assert (
            gen_option.vol_surface.params == expec_option.vol_surface.params
        ), "Vol surface params mismatch"
        assert (
            gen_option.vol_surface.model_type == expec_option.vol_surface.model_type
        ), "Vol surface model type mismatch"
        assert gen_option.time_type == expec_option.time_type, "Time type mistmatch"
        assert gen_option.vol_type == expec_option.vol_type, "Vol type mismatch"


@pytest.mark.parametrize(
    ["product", "populate_options", "populate_broken_dates", "months_forward"],
    [
        [Product(symbol="xlme-lad-usd", short_name="lad"), True, False, 18],
        [Product(symbol="xlme-lnd-usd", short_name="lnd"), False, True, 12],
    ],
)
def test_populate_full_curve(
    product,
    populate_options,
    populate_broken_dates,
    months_forward,
    mock_holidays,
):
    locked_current_datetime = datetime(2023, 9, 1, 12, tzinfo=ZoneInfo("Europe/London"))

    (
        lme_futures_curve,
        futures,
        options,
    ) = lme_staticdata_utils.generate_and_populate_futures_curve(
        product,
        mock_holidays,
        populate_options=populate_options,
        populate_broken_dates=populate_broken_dates,
        forward_months=months_forward,
        _current_datetime=locked_current_datetime,
    )

    if not populate_options:
        assert len(options) == 0, (
            "Options list length not zero even though they "
            "were not meant to populate"
        )
    assert len(futures) > len(options), "Options should only exist on monthly prompts"

    for option in options:
        logging.warning("Option symbol: %s, expiry: %s", option.symbol, option.expiry)
        option_future_expiry = option.expiry + relativedelta.relativedelta(
            days=14, hour=12, minute=30
        )
        logging.warning("Underlying predicted expiration: %s", option_future_expiry)
        assert (
            option_future_expiry in lme_futures_curve.monthlies
        ), "Option expiry + 14 days was not found in monthly set"
        assert (
            option.underlying_future.expiry in lme_futures_curve.monthlies
        ), "Option `underlying_future.expiry` not found in monthly set"
