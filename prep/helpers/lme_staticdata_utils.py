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
)
from zoneinfo import ZoneInfo

from prep.helpers import rjo_sftp_utils

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
