from prep.helpers import lme_staticdata_utils, date_name_utilities, rjo_sftp_utils
from upedata.static_data import Product, Future, Option
from upedata.dynamic_data import VolSurface
from upedata import enums as upe_enums

from pytest_mock import mocker, MockerFixture
import pandas as pd
import pytest

from typing import List, Dict, IO, Union
from datetime import datetime, date
from dateutil import relativedelta
from zoneinfo import ZoneInfo
import logging
import os


def stub_get_lme_overnight_data(base_file_name: str, fetch_most_recent_num=1):
    lme_inr_dfs = []
    lme_file_dts = []
    lme_inr_data_for_sorting = []

    for curr_path, dirs, files in os.walk("tests/lme_overnight_samples/"):
        for int_file_name in files:
            try:
                file_dt = datetime.strptime(int_file_name, "%Y%m%d_%H%M%S_INR.csv")
                with open(f"{curr_path}/{int_file_name}") as fp:
                    inr_df = pd.read_csv(fp, sep=",")
                    inr_df.columns = (
                        inr_df.columns.str.lower().str.strip().str.replace(" ", "_")
                    )
                    lme_inr_data_for_sorting.append((file_dt, inr_df))
            except ValueError:
                pass

    if fetch_most_recent_num > len(
        lme_inr_data_for_sorting
    ) or fetch_most_recent_num in (-1, 0):
        fetch_most_recent_num = len(lme_inr_data_for_sorting)

    lme_inr_data_sorted = sorted(
        lme_inr_data_for_sorting, key=lambda file_tuple: file_tuple[0], reverse=True
    )[0:fetch_most_recent_num]

    for file_dt, file_df in lme_inr_data_sorted:
        lme_file_dts.append(file_dt)
        lme_inr_dfs.append(file_df)

    return lme_file_dts, lme_inr_dfs


class MockParamikoSFTPClient:
    def __init__(self) -> None:
        self.curr_dir = os.path.abspath("./tests/rjo_sftp_simulator")
        self.original_dir = os.path.abspath(".")
        os.chdir(self.curr_dir)

    def chdir(self, directory: str, _strip_root_slash=True) -> None:
        if _strip_root_slash:
            directory = directory.lstrip(r"\/")
        if not os.path.exists(directory):
            raise ValueError(
                "Path given does not exist: " + self.curr_dir + "/" + directory
            )
        os.chdir(directory)
        self.curr_dir = os.path.abspath(".")

    def listdir(self, path=".") -> List[str]:
        os_listdir_res = os.listdir(path)
        logging.info("Current mock sftp directory list: %s", os_listdir_res)
        return os_listdir_res

    def open(self, filename: str, mode="r", bufsize=-1) -> IO:
        logging.warning("Trying to open %s", self.curr_dir + "/" + filename)
        mock_sftp_file = open(
            self.curr_dir + "/" + filename, mode=mode, buffering=bufsize
        )
        # this is a horrible bit of mockery to get prefetch appearing in the fake
        # sftp file so tests don't shit themselves.
        mock_sftp_file.prefetch = lambda: None  # type: ignore

        return mock_sftp_file

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        os.chdir(self.original_dir)


class MockParamikoClient:
    def __init__(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def open_sftp(self) -> MockParamikoSFTPClient:
        return MockParamikoSFTPClient()


def get_mock_paramiko_client() -> MockParamikoClient:
    return MockParamikoClient()


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
                    product_symbol="xlme-lad-usd",
                    product=Product(symbol="xlme-lad-usd", short_name="lad"),
                    expiry=datetime(2023, 11, 15, 19, 30),
                ),
                Future(
                    symbol="xlme-lad-usd f 23-11-14",
                    display_name="LAD 2023-11-14",
                    product_symbol="xlme-lad-usd",
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
        input_futures_list, input_futures_list[0].product, option_template_data
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


@pytest.mark.parametrize("num_to_pull", [1, -1, datetime(2023, 9, 26)])
def test_pull_lme_interest_rate_curve_ideal_data(
    num_to_pull: Union[int, datetime], mocker: MockerFixture
):
    mocker.patch(
        "prep.helpers.rjo_sftp_utils.get_rjo_ssh_client",
        new=get_mock_paramiko_client,
    )

    currencies = {"USD": "usd", "EUR": "eur", "GBP": "gbp", "JPY": "jpy"}
    (
        latest_dt,
        currencies_updated,
        interest_rates,
    ) = lme_staticdata_utils.pull_lme_interest_rate_curve(
        currencies, num_data_dates_to_pull=num_to_pull
    )

    for interest_rate_obj in interest_rates:
        assert interest_rate_obj.source == "LME", "Expected LME as interest rate source"
        assert interest_rate_obj.currency_symbol in list(
            currencies.values()
        ), "Unexpected currency_symbol"


@pytest.mark.parametrize("num_to_pull", [1, -1, datetime(2023, 9, 26)])
def test_pull_lme_futures_closing_prices_ideal_data(
    num_to_pull: Union[int, datetime], mocker: MockerFixture
):
    mocker.patch(
        "prep.helpers.rjo_sftp_utils.get_rjo_ssh_client",
        new=get_mock_paramiko_client,
    )

    (
        most_recent_closing_price_dt,
        most_recent_closing_price_df,
        closing_prices,
    ) = lme_staticdata_utils.pull_lme_futures_closing_price_data(
        num_data_dates_to_pull=num_to_pull
    )

    assert most_recent_closing_price_dt == datetime(
        2023, 9, 26
    ), "Most recent file had unexpected datetime"
    for closing_price in closing_prices:
        assert (
            closing_price.close_date <= most_recent_closing_price_dt.date()
        ), "Close date was more recent than most recent file"


@pytest.mark.parametrize("num_to_pull", [1, -1, datetime(2023, 9, 25)])
def test_pull_lme_options_closing_prices_ideal_data(
    num_to_pull: Union[int, datetime], mocker: MockerFixture
):
    mocker.patch(
        "prep.helpers.rjo_sftp_utils.get_rjo_ssh_client",
        new=get_mock_paramiko_client,
    )

    (
        most_recent_closing_price_dt,
        most_recent_closing_price_df,
        closing_prices,
    ) = lme_staticdata_utils.pull_lme_options_closing_price_data(
        num_data_dates_to_pull=num_to_pull
    )

    assert most_recent_closing_price_dt == datetime(
        2023, 9, 29
    ), "Most recent file had unexpected datetime"
    for closing_price in closing_prices:
        assert (
            closing_price.close_date <= most_recent_closing_price_dt.date()
        ), "Close date was more recent than most recent file"
