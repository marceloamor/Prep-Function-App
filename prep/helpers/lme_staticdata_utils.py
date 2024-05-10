import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import sqlalchemy.orm
import upedata.enums as upe_enums
from dateutil.relativedelta import WE, relativedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert
from upedata.dynamic_data import (
    ExchangeRate,
    FutureClosingPrice,
    InterestRate,
    OptionClosingPrice,
    VolSurface,
)
from upedata.static_data import (
    Future,
    FuturePriceFeedAssociation,
    Holiday,
    Option,
    PriceFeed,
    Product,
)
from upedata.template_language import parser
from zoneinfo import ZoneInfo

from prep.exceptions import ProductNotFound
from prep.helpers import lme_date_calc_funcs, rjo_sftp_utils

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
_DEFAULT_FORWARD_MONTHS = 18


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

        def within_broken_date_window(dt_to_check):
            return cash_date < dt_to_check < three_month_date

        logging.debug("Populating `LMEFuturesCurve` broken datetimes")
        cash_date = self.cash.date()
        three_month_date = self.three_month.date()
        europe_london_tz = ZoneInfo("Europe/London")
        expiry_time = time(19, 00, tzinfo=europe_london_tz)

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
        logging.debug("Generating prompt list from `LMEFuturesCurve`")
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


def gen_lme_futures(
    expiry_dates: List[datetime],
    product: Product,
    session: Optional[sqlalchemy.orm.Session] = None,
) -> List[Future]:
    static_data_futures: List[Future] = []
    product_3m_feed = None
    product_3m_relative_spread_feed = None
    if session is not None:
        product_3m_feed = session.get(
            PriceFeed, (LME_FUTURE_3M_FEED_ASSOC[product.short_name], "cqg")
        )
        product_3m_relative_spread_feed = session.get(
            PriceFeed, ("SPREAD_RELATIVE_TO_3M", "local")
        )
    if session is None or product_3m_feed is None:
        product_3m_feed = PriceFeed(
            feed_id=LME_FUTURE_3M_FEED_ASSOC[product.short_name],
            origin="cqg",
            delayed=False,
            subscribe=True,
        )
    if session is not None:
        session.add(product_3m_feed)
    if session is None or product_3m_relative_spread_feed is None:
        product_3m_relative_spread_feed = PriceFeed(
            feed_id="SPREAD_RELATIVE_TO_3M",
            origin="local",
            delayed=False,
            subscribe=False,
        )
    if session is not None:
        session.add(product_3m_relative_spread_feed)
    for expiry_date in expiry_dates:
        try:
            # this is terrible and inefficient *but* this runs once a day in the early
            # hours so it shouldn't matter too much
            if session is not None:
                new_lme_future = session.get(
                    Future, f"{product.symbol} f {expiry_date.strftime(r'%y-%m-%d')}"
                )
                if new_lme_future is not None:
                    static_data_futures.append(new_lme_future)
                    # session.add(new_lme_future)
                    continue
            product_3m_future_price_feed_assoc = FuturePriceFeedAssociation(
                feed=product_3m_feed,
                weighting=1.0,
            )
            product_relative_spread_feed = FuturePriceFeedAssociation(
                feed=product_3m_relative_spread_feed,
                weighting=1.0,
            )
            new_lme_future = Future(
                symbol=f"{product.symbol} f {expiry_date.strftime(r'%y-%m-%d')}",
                display_name=(
                    f"{product.short_name} {expiry_date.strftime(r'%Y-%m-%d')}"
                ).upper(),
                expiry=expiry_date,
                multiplier=LME_FUTURE_MULTIPLIERS[product.short_name],
                product_symbol=product.symbol,
                settlement={
                    "form": "physical",
                    "time": ["expiry", "0"],
                    "style": "forward",
                    "version": "1.1",
                },
            )
            new_lme_future.underlying_feeds = [
                product_3m_future_price_feed_assoc,
                product_relative_spread_feed,
            ]
            if session is not None:
                dict_future = new_lme_future.to_dict()
                new_lme_future = session.execute(
                    pg_insert(Future)
                    .values(dict_future)
                    .on_conflict_do_update(
                        index_elements=[Future.symbol], set_=dict_future
                    )
                    .returning(Future)
                ).scalar_one_or_none()
                session.add(product_3m_future_price_feed_assoc)
                session.add(product_relative_spread_feed)

        except KeyError:
            raise ProductNotFound(
                f"Unable to find {product.short_name} in `LME_FUTURE_MULTIPLIERS`"
            )
        if new_lme_future is not None:
            static_data_futures.append(new_lme_future)

    return static_data_futures


def gen_lme_options(
    futures_list: List[Future],
    product: Product,
    option_specification_data: Dict,
    session: Optional[sqlalchemy.orm.Session] = None,
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
                    option_specification_data["specific"][product.symbol.lower()]
                    | option_specification_data["shared"]
                )
                if session is not None:
                    # same comment from the gen futures function applies here!
                    generated_option = session.get(
                        Option,
                        f"{product.symbol} o {option_expiry_date.strftime(r'%y-%m-%d')} a",
                    )
                    if generated_option is not None:
                        generated_options.append(generated_option)
                        continue
                    else:
                        new_default_vol_surface_id = session.execute(
                            pg_insert(VolSurface)
                            .values(
                                {
                                    "model_type": general_option_data["vol_surface"][
                                        "model_type"
                                    ],
                                    "expiry": option_expiry_date,
                                    "params": general_option_data["vol_surface"][
                                        "params"
                                    ],
                                }
                            )
                            .returning(VolSurface.vol_surface_id)
                        ).scalar_one_or_none()
                else:
                    new_default_vol_surface_id = 0
                generated_option = Option(
                    symbol=f"{product.symbol} o {option_expiry_date.strftime(r'%y-%m-%d')} a",
                    multiplier=general_option_data["multiplier"],
                    strike_intervals=general_option_data["strike_intervals"],
                    expiry=option_expiry_date,
                    display_name=option_specification_data["shared"]["display_name"],
                    product_symbol=product.symbol,
                    underlying_future_symbol=future.symbol,
                    vol_surface_id=new_default_vol_surface_id,
                    vol_type=upe_enums.VolType(general_option_data["vol_type"]),
                    time_type=upe_enums.TimeType(general_option_data["time_type"]),
                    product=product,
                    underlying_future=future,
                )
                generated_option = parser.substitute_derivative_generation_time(
                    generated_option
                )
                if session is not None:
                    dict_option = generated_option.to_dict()
                    session.execute(
                        pg_insert(Option).values(dict_option).on_conflict_do_nothing()
                    )
                generated_options.append(generated_option)

    return generated_options


def populate_primary_curve_datetimes(
    non_prompts: List[date],
    product_holidays: List[Holiday],
    forward_months=_DEFAULT_FORWARD_MONTHS,
    _current_datetime=None,
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
    if not isinstance(_current_datetime, datetime):
        _current_datetime = datetime.now(tz=ZoneInfo("Europe/London"))
    lme_prompt_map = lme_date_calc_funcs.get_lme_prompt_map(
        non_prompts, _current_datetime=_current_datetime
    )
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
    session: Optional[sqlalchemy.orm.Session] = None,
    populate_options=True,
    populate_broken_dates=False,
    forward_months=_DEFAULT_FORWARD_MONTHS,
    _current_datetime=lambda: datetime.now(tz=ZoneInfo("Europe/London")),
) -> Tuple[LMEFuturesCurve, List[Future], List[Option]]:
    """Generates the futures and options (if `populate_options` is `True`) across the
    entire curve within the limit given by `months_forward`.

    Will provide all valid 3M and weekly prompt futures (runs to 6 months) and all monthly
    prompts to the limit of `months_forward`.

    When `session` is provided this function call will handle inserting new static data
    into the database by value using postgres specific on-conflict resolution queries.

    :param product: The static data represtentation of the LME product for which
    to generate derivatives
    :type product: Product
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param session: SQLAlchemy ORM Session object, optionally provided, defaults to None
    :type session: sqlalchemy.orm.Session, optional, defaults to None
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
        _current_datetime=_current_datetime
        if isinstance(_current_datetime, datetime)
        else _current_datetime(),
    )
    if populate_broken_dates:
        logging.info("Populating broken date datetimes for `%s`", product.symbol)
        lme_futures_curve.populate_broken_datetimes()

    future_expiries = lme_futures_curve.gen_prompt_list()
    futures = gen_lme_futures(future_expiries, product, session=session)
    logging.info("Now have %s valid futures for `%s`", len(futures), product.symbol)
    # if session is not None:
    #     session.add_all(futures)

    if populate_options:
        logging.info("Generating LME options for `%s`", product.symbol)
        options = gen_lme_options(
            futures, product, fetch_lme_option_specification_data(), session=session
        )
        logging.info("Now have %s valid options for `%s`", len(options), product.symbol)
        # if session is not None:
        #     session.add_all(options)
    else:
        options = []

    return lme_futures_curve, futures, options


def update_lme_product_static_data(
    lme_product: Product,
    sqla_session: sqlalchemy.orm.Session,
    first_run=False,
    placeholder_dt=None,
) -> LMEFuturesCurve:
    if not isinstance(placeholder_dt, datetime):
        (
            lme_futures_curve,
            futures,
            options,
        ) = generate_and_populate_futures_curve(
            lme_product,
            lme_product.holidays,
            populate_broken_dates=first_run,
            session=sqla_session,
        )
    else:
        (
            lme_futures_curve,
            futures,
            options,
        ) = generate_and_populate_futures_curve(
            lme_product,
            lme_product.holidays,
            populate_broken_dates=first_run,
            session=sqla_session,
            _current_datetime=placeholder_dt,
        )
    # sqla_session.add_all(futures)
    # sqla_session.add_all(options)

    return lme_futures_curve


def pull_lme_exchange_rates(
    currency_symbols_iso_unpaired: Set[str],
    num_data_dates_to_pull: Union[int, datetime],
) -> Tuple[datetime, List[ExchangeRate]]:
    current_dt = datetime.now(tz=ZoneInfo("Europe/London")).replace(hour=19)
    exchange_rate_datetimes, exchange_rate_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "EXR",
        num_recent_or_since_dt=num_data_dates_to_pull,
        date_cols_to_parse=["REPORT_DATE", "FORWARD_DATE"],
    )
    if len(exchange_rate_datetimes) == 0:
        return datetime(1970, 1, 1), []

    bulk_exchange_rates: List[ExchangeRate] = []

    for fx_rate_dt, fx_rate_df in zip(exchange_rate_datetimes, exchange_rate_dfs):
        fx_rate_df["currency_pair"] = (
            fx_rate_df["currency_pair"].str.strip().str.upper()
        )
        fx_rate_df["base_currency"] = fx_rate_df["currency_pair"].str[:3]
        fx_rate_df["quote_currency"] = fx_rate_df["currency_pair"].str[3:]
        # get all rows where the currencies involved are both in our requested
        # set provided in the function call
        fx_rate_df_filtered = fx_rate_df.loc[
            (
                fx_rate_df.loc[:, "base_currency"].isin(currency_symbols_iso_unpaired)
                & fx_rate_df.loc[:, "quote_currency"].isin(
                    currency_symbols_iso_unpaired
                )
                & (
                    fx_rate_df.loc[:, "base_currency"]
                    != fx_rate_df.loc[:, "quote_currency"]
                )
            )
        ]
        for row in fx_rate_df_filtered.loc[
            fx_rate_df_filtered["forward_date"].between(
                np.datetime64(fx_rate_dt, "ns"),
                np.datetime64(
                    current_dt + relativedelta(months=_DEFAULT_FORWARD_MONTHS - 1), "ns"
                ),
            )
        ].itertuples(index=False):
            bulk_exchange_rates.append(
                ExchangeRate(
                    published_date=fx_rate_dt.date(),
                    source="LME",
                    base_currency_symbol=row.base_currency.lower(),
                    quote_currency_symbol=row.quote_currency.lower(),
                    forward_date=row.forward_date,
                    rate=row.exchange_rate,
                )
            )

    return exchange_rate_datetimes[0], bulk_exchange_rates


def update_lme_exchange_rate_data(
    sqla_session: sqlalchemy.orm.Session,
    most_recent_datetime: Union[int, datetime],
    currencies_to_pull_iso_symbols: Set[str],
) -> datetime:
    # LME_CURRENCY_DATA = {"USD", "EUR", "GBP", "JPY"}
    df_dt, exchange_rates = pull_lme_exchange_rates(
        currencies_to_pull_iso_symbols, num_data_dates_to_pull=most_recent_datetime
    )
    exr_list_of_dicts: List[Dict[str, Any]] = []
    for exr_obj in exchange_rates:
        exr_list_of_dicts.append(exr_obj.to_dict())
    if len(exr_list_of_dicts) > 0:
        stmt = pg_insert(ExchangeRate).on_conflict_do_nothing()
        sqla_session.execute(stmt, exr_list_of_dicts)

    return df_dt


def pull_lme_interest_rate_curve(
    currencies_to_pull_iso_internal_sym: Dict[str, str],
    num_data_dates_to_pull: Union[int, datetime],
) -> Tuple[datetime, Set[str], List[InterestRate]]:
    # pandas is cancer and needs to be scorched from this Earth, it's a terrible library
    # with no place in modern software engineering, they can't even do bloody warnings properly
    pd.options.mode.chained_assignment = None
    current_dt = datetime.now(tz=ZoneInfo("Europe/London")).replace(hour=19)
    try:
        (
            interest_rate_datetimes,
            interest_rate_dfs,
        ) = rjo_sftp_utils.get_lme_overnight_data(
            "INR",
            num_recent_or_since_dt=num_data_dates_to_pull,
            date_cols_to_parse=["REPORT_DATE", "FORWARD_DATE"],
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
                1.0 + rate_dataframe.loc[:, "interest_rate"]
            )
            if rate_datetime == interest_rate_datetimes[0]:
                for currency_iso in rate_dataframe.currency.unique():
                    most_recent_updated_currencies.add(currency_iso)
            for row in rate_dataframe.loc[
                rate_dataframe["forward_date"].between(
                    np.datetime64(rate_datetime, "ns"),
                    np.datetime64(
                        current_dt + relativedelta(months=_DEFAULT_FORWARD_MONTHS - 1),
                        "ns",
                    ),
                )
            ].itertuples(index=False):
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
    except Exception as e:
        pd.options.mode.chained_assignment = "warn"
        raise e
    pd.options.mode.chained_assignment = "warn"

    return (
        interest_rate_datetimes[0],
        most_recent_updated_currencies,
        bulk_interest_rate_data,
    )


def update_lme_interest_rate_static_data(
    sqla_session: sqlalchemy.orm.Session,
    most_recent_datetime: Union[int, datetime],
) -> Tuple[datetime, Set[str]]:
    LME_CURRENCY_DATA = {"USD": "usd", "EUR": "eur", "GBP": "gbp", "JPY": "jpy"}
    df_dt, updated_currencies, interest_rates = pull_lme_interest_rate_curve(
        LME_CURRENCY_DATA, num_data_dates_to_pull=most_recent_datetime
    )
    interest_rate_list_of_dicts: List[Dict[str, Any]] = []
    for interest_rate_obj in interest_rates:
        interest_rate_list_of_dicts.append(interest_rate_obj.to_dict())

    if len(interest_rates) > 0:
        stmt = pg_insert(InterestRate).on_conflict_do_nothing()
        sqla_session.execute(stmt, interest_rate_list_of_dicts)
    else:
        pass

    return df_dt, updated_currencies


def pull_lme_options_closing_price_data(
    num_data_dates_to_pull: Union[int, datetime],
) -> Tuple[datetime, pd.DataFrame, List[OptionClosingPrice]]:
    current_dt = datetime.now(tz=ZoneInfo("Europe/London")).replace(hour=19)
    closing_price_datetimes, closing_price_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "CLO",
        num_recent_or_since_dt=num_data_dates_to_pull,
        date_cols_to_parse=["REPORT_DATE", "FORWARD_DATE"],
    )
    if len(closing_price_datetimes) == 0:
        return (datetime(1970, 1, 1), pd.DataFrame(), [])

    bulk_closing_prices: List[OptionClosingPrice] = []
    for closing_price_dt, closing_price_df in zip(
        closing_price_datetimes, closing_price_dfs
    ):
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
            lambda yyyy_mm_int: np.datetime64(
                datetime.strptime(f"{str(int(yyyy_mm_int))}01", r"%Y%m%d")
                + relativedelta(weekday=WE(1)),
                "ns",
            )
        )
        pd.options.mode.chained_assignment = "warn"
        for row in closing_price_df.loc[
            closing_price_df["expiry_date"].between(
                np.datetime64(closing_price_dt, "ns"),
                np.datetime64(
                    current_dt + relativedelta(months=_DEFAULT_FORWARD_MONTHS - 1), "ns"
                ),
            )
        ].itertuples(index=False):
            option_internal_identifier = LME_PRODUCT_IDENTIFIER_MAP[
                row.contract.upper()
            ]
            bulk_closing_prices.append(
                OptionClosingPrice(
                    close_date=row.report_date.date(),
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
    logging.info("Found %s option closing prices", len(bulk_closing_prices))

    return closing_price_datetimes[0], closing_price_dfs[0], bulk_closing_prices


def pull_lme_futures_closing_price_data(
    num_data_dates_to_pull: Union[int, datetime],
) -> Tuple[datetime, pd.DataFrame, List[FutureClosingPrice]]:
    current_dt = datetime.now(tz=ZoneInfo("Europe/London")).replace(hour=19)
    closing_price_datetimes, closing_price_dfs = rjo_sftp_utils.get_lme_overnight_data(
        "FCP",
        num_recent_or_since_dt=num_data_dates_to_pull,
        date_cols_to_parse=["REPORT_DATE", "FORWARD_DATE"],
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
        for row in closing_price_df.loc[
            closing_price_df["forward_date"].between(
                np.datetime64(closing_price_datetime, "ns"),
                np.datetime64(
                    current_dt + relativedelta(months=_DEFAULT_FORWARD_MONTHS - 1), "ns"
                ),
            )
        ].itertuples(index=False):
            try:
                future_internal_ident = LME_PRODUCT_IDENTIFIER_MAP[
                    f"{row.underlying}D"
                ].lower()
            except KeyError:
                logging.debug(
                    "Passed on row with underlying %s as it is currently not listed for ingest",
                    row.underlying,
                )
                continue
            future_exp_str = row.forward_date.strftime(r"%y-%m-%d")
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
    most_recent_datetime: Union[int, datetime],
) -> Tuple[datetime, pd.DataFrame]:
    (
        most_recent_dt,
        most_recent_df,
        future_closing_prices,
    ) = pull_lme_futures_closing_price_data(num_data_dates_to_pull=most_recent_datetime)
    fcp_list_of_dicts: List[Dict[str, Any]] = []
    for fcp_obj in future_closing_prices:
        fcp_list_of_dicts.append(fcp_obj.to_dict())
    if len(fcp_list_of_dicts) > 0:
        stmt = pg_insert(FutureClosingPrice).on_conflict_do_nothing()
        sqla_session.execute(stmt, fcp_list_of_dicts)

    return most_recent_dt, most_recent_df


def update_lme_options_closing_price_data(
    sqla_session: sqlalchemy.orm.Session,
    most_recent_datetime: Union[int, datetime],
) -> Tuple[datetime, pd.DataFrame]:
    (
        most_recent_dt,
        most_recent_df,
        option_closing_prices,
    ) = pull_lme_options_closing_price_data(num_data_dates_to_pull=most_recent_datetime)
    clo_list_of_dicts: List[Dict[str, Any]] = []
    for clo_obj in option_closing_prices:
        clo_list_of_dicts.append(clo_obj.to_dict())
    if len(clo_list_of_dicts) > 0:
        stmt = pg_insert(OptionClosingPrice).on_conflict_do_nothing()
        sqla_session.execute(stmt, clo_list_of_dicts)

    return most_recent_dt, most_recent_df
