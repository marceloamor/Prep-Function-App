from prep.helpers import lme_date_calc_funcs, rjo_sftp_utils
from prep.exceptions import ProductNotFound

from upedata.static_data import (
    FuturePriceFeedAssociation,
    PriceFeed,
    Product,
    Holiday,
    Future,
    Option,
)
from upedata.dynamic_data import (
    FutureClosingPrice,
    OptionClosingPrice,
    InterestRate,
    VolSurface,
)
from upedata.template_language import parser
import upedata.enums as upe_enums

from dateutil.relativedelta import relativedelta, WE
import sqlalchemy.orm
import pandas as pd
import numpy as np

from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime, date, time
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
import logging
import json


LME_PRODUCT_NAMES = ["AHD", "CAD", "PBD", "ZSD", "NID"]
LME_METAL_NAMES = ["aluminium", "copper", "lead", "zinc", "nickel"]
LME_FUTURE_MULTIPLIERS_LIST = [25, 25, 25, 25, 6]
GEORGIA_LME_PRODUCT_NAMES_BASE = ["lad", "lcu", "pbd", "lzh", "lnd"]
CQG_3M_FEEDS = ["X.US.LALZ", "X.US.LDKZ", "X.US.LEDZ", "X.US.LZHZ", "X.US.LNIZ"]
LME_FUTURE_MULTIPLIERS = {
    georgia_product_name: product_multiplier
    for georgia_product_name, product_multiplier in zip(
        GEORGIA_LME_PRODUCT_NAMES_BASE, LME_FUTURE_MULTIPLIERS_LIST
    )
}
LME_FUTURE_3M_FEED_ASSOC = {
    georgia_product_name: future_3m_feed
    for georgia_product_name, future_3m_feed in zip(
        GEORGIA_LME_PRODUCT_NAMES_BASE, CQG_3M_FEEDS
    )
}
LME_PRODUCT_IDENTIFIER_MAP = {
    lme_product_name: georgia_product_name
    for lme_product_name, georgia_product_name in zip(
        LME_PRODUCT_NAMES, GEORGIA_LME_PRODUCT_NAMES_BASE
    )
}
LME_PRODUCT_NAME_MAP = {
    lme_product_name[0:2]: lme_metal_name
    for lme_product_name, lme_metal_name in zip(LME_PRODUCT_NAMES, LME_METAL_NAMES)
}

logger = logging.getLogger("prep.helpers.lme_staticdata_utils")


@dataclass
class LMEFuturesCurve:
    cash: datetime
    three_month: datetime
    weeklies: List[datetime]
    monthlies: List[datetime]
    prompt_map: Dict[date, date]
    tom: Optional[datetime] = None
    broken_dates: List[datetime] = field(default_factory=list)

    # possibly the most disgustingly *pythonic* thing I've ever written...
    # sorry to anyone that has to look at this
    def populate_broken_datetimes(self):
        """Populates the broken dates within the curve in-place, will
        not include TOM, CASH, or 3M, but may overlap with monthlies
        or weeklies.
        """
        logger.debug("Populating `LMEFuturesCurve` broken datetimes")
        cash_date = self.cash.date()
        three_month_date = self.three_month.date()
        europe_london_tz = ZoneInfo("Europe/London")
        expiry_time = time(12, 30, tzinfo=europe_london_tz)
        within_broken_date_window = (
            lambda dt_to_check: cash_date < dt_to_check < three_month_date
        )
        self.broken_dates = sorted(
            {
                datetime.combine(filtered_date, expiry_time)
                for filtered_date in set(self.prompt_map.values())
                if within_broken_date_window(filtered_date)
            }
        )

    def gen_prompt_list(self) -> List[datetime]:
        """Returns a sorted list of all prompts contained in this
        dataclass

        :return: Sorted list of prompt datetimes
        :rtype: List[datetime]
        """
        logger.debug("Generating prompt list from `LMEFuturesCurve`")
        prompt_set = set(
            [self.cash, self.three_month]
            + self.weeklies
            + self.monthlies
            + self.broken_dates
        )
        if self.tom is not None:
            prompt_set.add(self.tom)
        return sorted(list(prompt_set))


def fetch_lme_option_specification_data(
    path="./prep/helpers/data_files/lme_option_base_data.json",
) -> Dict:
    with open(path, "r") as fp:
        option_spec_data = json.load(fp)
    return option_spec_data


def gen_lme_futures(expiry_dates: List[datetime], product: Product) -> List[Future]:
    static_data_futures: List[Future] = []
    product_3m_feed = PriceFeed(
        feed_id=LME_FUTURE_3M_FEED_ASSOC[product.short_name],
        origin="cqg",
        delayed=False,
        subscribe=True,
    )
    product_3m_relative_spread_feed = PriceFeed(
        feed_id="SPREAD_RELATIVE_TO_3M",
        origin="local",
        delayed=False,
        subscribe=False,
    )
    for expiry_date in expiry_dates:
        try:
            new_lme_future = Future(
                symbol=f"{product.symbol} f {expiry_date.strftime(r'%y-%m-%d')}",
                display_name=(
                    f"{product.short_name} {expiry_date.strftime(r'%Y-%m-%d')}"
                ).upper(),
                expiry=expiry_date,
                multiplier=LME_FUTURE_MULTIPLIERS[product.short_name],
                product=product,
            )
            product_3m_future_price_feed_assoc = FuturePriceFeedAssociation(
                future=new_lme_future,
                feed=product_3m_feed,
                weighting=1.0,
            )
            product_relative_spread_feed = FuturePriceFeedAssociation(
                future=new_lme_future,
                feed=product_3m_relative_spread_feed,
                weighting=1.0,
            )
            new_lme_future.underlying_feeds = [
                product_3m_future_price_feed_assoc,
                product_relative_spread_feed,
            ]

        except KeyError:
            raise ProductNotFound(
                f"Unable to find {product.short_name} in `LME_FUTURE_MULTIPLIERS`"
            )
        static_data_futures.append(new_lme_future)

    return static_data_futures


def gen_lme_options(
    futures_list: List[Future], option_specification_data: Dict
) -> List[Option]:
    two_week_td = relativedelta(days=14)
    generated_options: List[Option] = []
    for future in futures_list:
        # first need to check to make sure it's on a third Wednesday, as these will
        # always be underlying of an LME option.
        # if not just ignore
        if future.expiry.weekday() == 2:
            if 15 <= future.expiry.day and future.expiry.day <= 21:
                # the timezone fuckery is in case of something *somehow* crossing a dst
                # boundary, odds are this won't happen but you never know.
                option_expiry_date = (
                    future.expiry.replace(tzinfo=ZoneInfo("Europe/London"))
                    - two_week_td
                ).replace(hour=11, minute=15)
                general_option_data = (
                    option_specification_data["specific"][future.product.symbol.lower()]
                    | option_specification_data["shared"]
                )
                generated_option = Option(
                    symbol=f"{future.product.symbol} o {option_expiry_date.strftime(r'%y-%m-%d')} a",
                    multiplier=general_option_data["multiplier"],
                    strike_intervals=general_option_data["strike_intervals"],
                    expiry=option_expiry_date,
                    display_name=option_specification_data["shared"]["display_name"],
                    product=future.product,
                    underlying_future=future,
                    vol_surface=VolSurface(
                        model_type=general_option_data["vol_surface"]["model_type"],
                        expiry=option_expiry_date,
                        params=general_option_data["vol_surface"]["params"],
                    ),
                    vol_type=upe_enums.VolType(general_option_data["vol_type"]),
                    time_type=upe_enums.TimeType(general_option_data["time_type"]),
                )
                generated_option = parser.substitute_derivative_generation_time(
                    generated_option
                )
                generated_options.append(generated_option)

    return generated_options


def populate_primary_curve_datetimes(
    non_prompts: List[date],
    product_holidays: List[Holiday],
    forward_months=18,
    _current_datetime=datetime.now(tz=ZoneInfo("Europe/London")),
) -> LMEFuturesCurve:
    """Generates and populates a container dataclass with the primary
    prompt dates associated with a given LME product, this will
    provide: TOM, CASH, 3M, weeklies, and monthlies with no guarantee
    of uniqueness of prompt between these fields.

    :param non_prompts: List of all LME non-prompt dates
    :type non_prompts: List[date]
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param populate_options: Whether to generate options associated with generated monthly
    futures, defaults to True
    :type populate_options: bool, optional
    :param forward_months: Number of months of monthly futures to generate, also corresponds
    to the number of options generated as these are derivative of the monthly futures,
    defaults to 18
    :type forward_months: int, optional
    :return: Container for LME product future prompt datetimes
    :rtype: LMEFuturesCurve
    """
    lme_prompt_map = lme_date_calc_funcs.get_lme_prompt_map(non_prompts)
    lme_3m_datetime = lme_date_calc_funcs.get_3m_datetime(
        _current_datetime, lme_prompt_map
    )
    lme_cash_datetime = lme_date_calc_funcs.get_cash_datetime(
        _current_datetime, product_holidays
    )
    lme_tom_datetime = lme_date_calc_funcs.get_tom_datetime(
        _current_datetime, product_holidays
    )
    lme_weekly_datetimes = lme_date_calc_funcs.get_all_valid_weekly_prompts(
        _current_datetime, lme_prompt_map
    )
    lme_monthly_datetimes = lme_date_calc_funcs.get_valid_monthly_prompts(
        _current_datetime, forward_months=forward_months
    )
    return LMEFuturesCurve(
        lme_cash_datetime,
        lme_3m_datetime,
        lme_weekly_datetimes,
        lme_monthly_datetimes,
        lme_prompt_map,
        tom=lme_tom_datetime,
    )


def generate_and_populate_futures_curve(
    product: Product,
    product_holidays: List[Holiday],
    populate_options=True,
    populate_broken_dates=False,
    forward_months=18,
    _current_datetime=datetime.now(tz=ZoneInfo("Europe/London")),
) -> Tuple[LMEFuturesCurve, List[Future], List[Option]]:
    """Generates the futures and options (if `populate_options` is `True`) across the
    entire curve within the limit given by `months_forward`.
    Will provide all valid 3M and weekly prompt futures (runs to 6 months) and all monthly
    prompts to the limit of `months_forward`.

    :param product: The static data represtentation of the LME product for which
    to generate derivatives
    :type product: Product
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param populate_options: Whether to generate options associated with generated monthly
    futures, defaults to True
    :type populate_options: bool, optional
    :param populate_broken_dates: Whether to populate and generate broken dated futures,
    defaults to False
    :type populate_broken_dates: bool, optional
    :param forward_months: Number of months of monthly futures to generate, also corresponds
    to the number of options generated as these are derivative of the monthly futures,
    defaults to 18
    :type forward_months: int, optional
    :return: A tuple containing the LMEFuturesCurve, a list of all futures generated and
    the list of all options generated, the latter will be an empty list if
    `populate_options is False`
    :rtype: Tuple[LMEFuturesCurve, List[Future], List[Option]]
    """

    non_prompts = [holiday.holiday_date for holiday in product_holidays]

    lme_futures_curve = populate_primary_curve_datetimes(
        non_prompts,
        product_holidays,
        forward_months=forward_months,
        _current_datetime=_current_datetime,
    )
    if populate_broken_dates:
        lme_futures_curve.populate_broken_datetimes()

    future_expiries = lme_futures_curve.gen_prompt_list()
    futures = gen_lme_futures(future_expiries, product)
    if populate_options:
        options = gen_lme_options(futures, fetch_lme_option_specification_data())
    else:
        options = []

    return lme_futures_curve, futures, options


def update_lme_product_static_data(
    lme_product: Product,
    sqla_session: sqlalchemy.orm.Session,
    first_run=False,
) -> LMEFuturesCurve:
    (
        lme_futures_curve,
        futures,
        options,
    ) = generate_and_populate_futures_curve(
        lme_product, lme_product.holidays, populate_broken_dates=first_run
    )
    sqla_session.add_all(futures)
    sqla_session.add_all(options)

    return lme_futures_curve


def pull_lme_interest_rate_curve(
    currencies_to_pull_iso_internal_sym: Dict[str, str], num_data_dates_to_pull=1
) -> Tuple[datetime, Set[str], List[InterestRate]]:
    # pandas is cancer and needs to be scorched from this Earth, it's a terrible library
    # with no place in modern software engineering, they can't even do bloody warnings properly
    pd.options.mode.chained_assignment = None
    interest_rate_datetimes, interest_rate_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "INR", num_recent_or_since_dt=num_data_dates_to_pull
    )
    if len(interest_rate_datetimes) == 0:
        return datetime(1970, 1, 1), set(), []
    bulk_interest_rate_data: List[InterestRate] = []
    # yes of course this isn't the most efficient O(whatever the fuck) implementation
    # but this code in production will be running twice a day at most so I don't really care
    valid_currencies_iso = list(currencies_to_pull_iso_internal_sym.keys())
    most_recent_updated_currencies = set()
    for rate_datetime, rate_dataframe in zip(
        interest_rate_datetimes, interest_rate_dfs
    ):
        rate_dataframe = rate_dataframe[
            rate_dataframe["currency"].str.upper().isin(valid_currencies_iso)
        ]
        rate_dataframe.loc[:, "continuous_rate"] = np.log(
            1.0 + rate_dataframe.interest_rate
        )
        if rate_datetime == interest_rate_datetimes[0]:
            for currency_iso in rate_dataframe.currency.unique():
                most_recent_updated_currencies.add(currency_iso)
        for row in rate_dataframe.itertuples(index=False):
            bulk_interest_rate_data.append(
                InterestRate(
                    published_date=row.report_date,
                    to_date=row.forward_date,
                    currency_symbol=currencies_to_pull_iso_internal_sym[
                        row.currency.upper()
                    ],
                    source="LME",
                    continuous_rate=row.continuous_rate,
                )
            )
    pd.options.mode.chained_assignment = "warn"

    return (
        interest_rate_datetimes[0],
        most_recent_updated_currencies,
        bulk_interest_rate_data,
    )


def update_lme_interest_rate_static_data(
    sqla_session: sqlalchemy.orm.Session, first_run=False
) -> Tuple[datetime, Set[str]]:
    LME_CURRENCY_DATA = {"USD": "usd", "EUR": "eur", "GBP": "gbp", "JPY": "jpy"}
    num_dates_to_pull = -1 if first_run else 1
    df_dt, updated_currencies, interest_rates = pull_lme_interest_rate_curve(
        LME_CURRENCY_DATA, num_data_dates_to_pull=num_dates_to_pull
    )
    sqla_session.add_all(interest_rates)

    return df_dt, updated_currencies


def pull_lme_options_closing_price_data(
    num_data_dates_to_pull=1,
) -> Tuple[datetime, pd.DataFrame, List[OptionClosingPrice]]:
    closing_price_datetimes, closing_price_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "CLO", num_recent_or_since_dt=num_data_dates_to_pull
    )
    if len(closing_price_datetimes) == 0:
        return (datetime(1970, 1, 1), pd.DataFrame(), [])

    bulk_closing_prices: List[OptionClosingPrice] = []
    for closing_price_df in closing_price_dfs:
        # filters for just those within the contracts we trade that are option
        # closing prices
        closing_price_df = closing_price_df[
            (closing_price_df["contract_type"].str.upper() == "LMEOPTION")
            & (closing_price_df["price_type"].str.upper() == "CLOSING")
            & (closing_price_df["contract"].str.upper().isin(LME_PRODUCT_NAMES))
        ]
        pd.options.mode.chained_assignment = None
        closing_price_df.loc[:, "expiry_date"] = closing_price_df.loc[
            :, "forward_month"
        ].apply(
            lambda yyyy_mm_int: (
                datetime.strptime(f"{str(int(yyyy_mm_int))}01", r"%Y%m%d")
                + relativedelta(weekday=WE(1))
            ).date()
        )
        pd.options.mode.chained_assignment = "warn"
        for row in closing_price_df.itertuples(index=False):
            option_internal_identifier = LME_PRODUCT_IDENTIFIER_MAP[
                row.contract.upper()
            ]
            bulk_closing_prices.append(
                OptionClosingPrice(
                    close_date=datetime.strptime(
                        str(row.report_date), r"%Y%m%d"
                    ).date(),
                    option_symbol=f"xlme-{option_internal_identifier}-usd o {row.expiry_date.strftime(r'%y-%m-%d')} a",
                    option_strike=float(row.strike),
                    call_or_put=upe_enums.CallOrPut.CALL
                    if row.sub_contract_type == "C"
                    else upe_enums.CallOrPut.PUT,
                    close_price=row.price,
                    close_volatility=row.volatility,
                    close_delta=row.delta,
                )
            )

    return closing_price_datetimes[0], closing_price_dfs[0], bulk_closing_prices


def pull_lme_futures_closing_price_data(
    num_data_dates_to_pull=1,
) -> Tuple[datetime, pd.DataFrame, List[FutureClosingPrice]]:
    closing_price_datetimes, closing_price_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "FCP", num_recent_or_since_dt=num_data_dates_to_pull
    )
    if len(closing_price_datetimes) == 0:
        return datetime(1970, 1, 1), pd.DataFrame(), []
    bulk_closing_prices: List[FutureClosingPrice] = []
    for closing_price_datetime, closing_price_df in zip(
        closing_price_datetimes, closing_price_dfs
    ):
        closing_price_df = closing_price_df[
            (closing_price_df["currency"].str.upper() == "USD")
            & (closing_price_df["price_type"].str.upper() == "FC")
        ]
        for row in closing_price_df.itertuples(index=False):
            try:
                future_internal_ident = LME_PRODUCT_IDENTIFIER_MAP[
                    f"{row.underlying}D"
                ].lower()
            except KeyError:
                logger.debug(
                    "Passed on row with underlying %s as it is currently not listed for ingest",
                    row.underlying,
                )
                continue
            future_exp_str = datetime.strptime(
                str(row.forward_date), r"%Y%m%d"
            ).strftime(r"%y-%m-%d")
            bulk_closing_prices.append(
                FutureClosingPrice(
                    close_date=closing_price_datetime.date(),
                    future_symbol=f"xlme-{future_internal_ident}-usd f {future_exp_str}",
                    close_price=row.price,
                )
            )

    return closing_price_datetimes[0], closing_price_dfs[0], bulk_closing_prices


def update_lme_futures_closing_price_data(
    sqla_session: sqlalchemy.orm.Session,
    first_run=False,
) -> Tuple[datetime, pd.DataFrame]:
    num_dates_to_pull = -1 if first_run else 1
    (
        most_recent_dt,
        most_recent_df,
        future_closing_prices,
    ) = pull_lme_futures_closing_price_data(num_data_dates_to_pull=num_dates_to_pull)
    sqla_session.add_all(future_closing_prices)
    return most_recent_dt, most_recent_df


def update_lme_options_closing_price_data(
    sqla_session: sqlalchemy.orm.Session,
    first_run=False,
) -> Tuple[datetime, pd.DataFrame]:
    num_dates_to_pull = -1 if first_run else 1
    (
        most_recent_dt,
        most_recent_df,
        option_closing_prices,
    ) = pull_lme_options_closing_price_data(num_data_dates_to_pull=num_dates_to_pull)
    sqla_session.add_all(option_closing_prices)
    return most_recent_dt, most_recent_df
