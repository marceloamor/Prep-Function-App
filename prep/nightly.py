from prep.helpers import lme_staticdata_utils, time_series_interpolation
from prep import handy_dandy_variables
from exceptions import ProductNotFound

from upedata.dynamic_data import InterestRate
from upedata.static_data import Exchange

from dateutil import relativedelta
import sqlalchemy.orm
import pandas as pd
import sqlalchemy
import redis
import ujson

from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import os


redis_dev_key_append = handy_dandy_variables.redis_key_append

LME_3M_DATE_KEYS = ujson.loads(
    os.getenv("LME_3M_DATE_LOCATIONS_REDIS", '["3m", "lme:3m_date"]')
)
LME_CASH_DATE_KEYS = ujson.loads(
    os.getenv("LME_CASH_DATE_LOCATIONS_REDIS", '["lme:cash_date"]')
)
LME_TOM_DATE_KEYS = ujson.loads(
    os.getenv("LME_TOM_DATE_LOCATIONS_REDIS", '["lme:tom_date"]')
)
LEGACY_LME_INR_RECENCY_KEY = os.getenv("LEGACY_LME_INR_RECENCY_KEY", "INR_update")
LEGACY_LME_FCP_RECENCY_KEY = os.getenv("LEGACY_LME_FCP_RECENCY_KEY", "FCP_update")
LEGACY_LME_CLO_RECENCY_KEY = os.getenv("LEGACY_LME_CLO_RECENCY_KEY", "CLO_update")
LME_INR_RECENCY_KEY = os.getenv("LME_INR_RECENCY_KEY", "prep:health:lme:inr")
LME_FCP_RECENCY_KEY = os.getenv("LME_FCP_RECENCY_KEY", "prep:health:lme:fcp")
LME_CLO_RECENCY_KEY = os.getenv("LME_CLO_RECENCY_KEY", "prep:health:lme:clo")

PREP_USD_RECENCY_KEY = os.getenv("PREP_USD_RECENCY_KEY", "prep:health:rates:usd")
PREP_GBP_RECENCY_KEY = os.getenv("PREP_GBP_RECENCY_KEY", "prep:health:rates:gbp")
PREP_EUR_RECENCY_KEY = os.getenv("PREP_EUR_RECENCY_KEY", "prep:health:rates:eur")
PREP_JPY_RECENCY_KEY = os.getenv("PREP_JPY_RECENCY_KEY", "prep:health:rates:jpy")

UPDATED_CURRENCY_TO_KEY = {
    "USD": PREP_USD_RECENCY_KEY,
    "GBP": PREP_GBP_RECENCY_KEY,
    "EUR": PREP_EUR_RECENCY_KEY,
    "JPY": PREP_JPY_RECENCY_KEY,
}
LME_FCP_PRODUCT_TO_REDIS_KEY = {
    lme_product_name[0:2]: f"lme:xlme-{georgia_product_name}-usd:fcp"
    for lme_product_name, georgia_product_name in zip(
        lme_staticdata_utils.LME_PRODUCT_NAMES,
        lme_staticdata_utils.GEORGIA_LME_PRODUCT_NAMES_BASE,
    )
}


def update_lme_relative_forward_dates(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    now_london_datetime = datetime.now(
        tz=ZoneInfo("Europe/London")
    ) + relativedelta.relativedelta(hours=6)
    if now_london_datetime.weekday >= 5:
        logging.info("No updating 3M date on weekends")
        return

    with sqlalchemy.orm.Session(engine) as session:
        lme_exchange = session.get(Exchange, "xlme")
        if lme_exchange is None:
            raise ValueError("Unable to find LME exchange under symbol `xlme`")
        # this is based on the assumption that all LME products share the
        cached_futures_curve_data = None
        for lme_product in lme_exchange.products:
            lme_futures_curve_data = (
                lme_staticdata_utils.update_lme_product_static_data(
                    lme_product, redis_conn, engine, first_run=first_run
                )
            )
            if (
                lme_product.short_name.lower() in ["lcu", "lad", "lnd", "lzh", "pbd"]
                and cached_futures_curve_data is None
            ):
                cached_futures_curve_data = lme_futures_curve_data

        if cached_futures_curve_data is None:
            raise ProductNotFound(
                'Unable to find any of `["lcu", "lad", "lnd", "lzh", "pbd"]` '
                "in xlme product short names"
            )

        lme_3m_datetime = cached_futures_curve_data.three_month
        lme_cash_datetime = cached_futures_curve_data.cash
        lme_tom_datetime = cached_futures_curve_data.tom

        session.commit()

    redis_pipeline = redis_conn.pipeline()
    for key in LME_3M_DATE_KEYS:
        redis_pipeline.set(
            key + redis_dev_key_append, lme_3m_datetime.strftime(r"%Y%m%d")
        )
    for key in LME_CASH_DATE_KEYS:
        redis_pipeline.set(
            key + redis_dev_key_append, lme_cash_datetime.strftime(r"%Y%m%d")
        )
    for key in LME_TOM_DATE_KEYS:
        redis_pipeline.set(
            key + redis_dev_key_append, lme_tom_datetime.strftime(r"%Y%m%d")
        )

    redis_pipeline.execute()


def update_currency_interest_curves_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    rate_curve_data = {
        currency_iso_sym: {"legacy": {}, "new": {}}
        for currency_iso_sym in list(UPDATED_CURRENCY_TO_KEY.keys())
    }

    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_INR_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.strptime(most_recent_file, r"%Y%m%d")
        except ValueError:
            pass
    with sqlalchemy.orm.Session(engine) as session:
        (
            most_recent_rate_datetime,
            updated_currencies,
        ) = lme_staticdata_utils.update_lme_interest_rate_static_data(
            session, most_recent_datetime=num_to_pull_or_dt
        )
        select_most_recent_inr_curve = (
            sqlalchemy.select(InterestRate.to_date, InterestRate.continuous_rate)
            .where(InterestRate.source == "LME")
            .where(InterestRate.published_date == most_recent_rate_datetime.date())
        )
        session.commit()
        for curr_iso_sym, rate_data in rate_curve_data.items():
            interest_rates = (
                session.execute(
                    select_most_recent_inr_curve.where(
                        InterestRate.currency_symbol == curr_iso_sym.lower()
                    )
                )
                .scalars()
                .all()
            )
            interest_rate_df = pd.DataFrame.from_records(
                interest_rates, index="date", columns=["date", "cont_rate"]
            )
            interest_rate_df.index = pd.DatetimeIndex(data=interest_rate_df.index)
            interest_rate_df = time_series_interpolation.interpolate_on_time_series_df(
                interest_rate_df,
                "cont_rate",
                "interp_cont_rate",
            )
            for interest_rate_row_data in interest_rate_df.itertuples():
                rate_data["legacy"][
                    interest_rate_row_data.Index.strftime(r"%Y%m%d")
                ] = {"Interest Rate": interest_rate_row_data.cont_rate}
                rate_data["new"][
                    interest_rate_row_data.Index.strftime(r"%Y%m%d")
                ] = interest_rate_row_data.cont_rate
        session.commit()

    redis_pipeline = redis_conn.pipeline()
    most_recent_dt_Ymd = most_recent_rate_datetime.strftime(r"%Y%m%d")
    for updated_currency_iso in updated_currencies:
        redis_pipeline.set(
            f"{updated_currency_iso.upper()}Rate{redis_dev_key_append}",
            ujson.dumps(rate_curve_data[updated_currency_iso.upper()]["legacy"]),
        )
        redis_pipeline.set(
            UPDATED_CURRENCY_TO_KEY[updated_currency_iso.upper()]
            + redis_dev_key_append,
            most_recent_dt_Ymd,
        )
    redis_pipeline.set(
        LEGACY_LME_INR_RECENCY_KEY + redis_dev_key_append, most_recent_dt_Ymd
    )
    redis_pipeline.set(LME_INR_RECENCY_KEY + redis_dev_key_append, most_recent_dt_Ymd)
    redis_pipeline.execute()


def update_future_closing_prices_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_FCP_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.strptime(most_recent_file, r"%Y%m%d")
        except ValueError:
            pass
    with sqlalchemy.orm.Session(engine) as session:
        (
            most_recent_file_dt,
            most_recent_file_df,
        ) = lme_staticdata_utils.update_lme_futures_closing_price_data(
            session, most_recent_datetime=num_to_pull_or_dt
        )
        if most_recent_file_dt == datetime(1970, 1, 1):
            return
        session.commit()
        most_recent_file_df = most_recent_file_df[
            (most_recent_file_df["currency"] == "USD")
            & (most_recent_file_df["price_type"] == "FC")
        ]
        most_recent_file_df.loc[:, "prompt_date"] = most_recent_file_df.loc[
            :, "forward_date"
        ].apply(lambda forward_date: datetime.strptime(str(forward_date), r"%Y%m%d"))

        redis_pipeline = redis_conn.pipeline()
        for underlying_no_curr, redis_key in LME_FCP_PRODUCT_TO_REDIS_KEY.items():
            product_specific_df = most_recent_file_df[
                most_recent_file_df["underlying"] == underlying_no_curr
            ]
            product_specific_df.index = pd.DatetimeIndex(
                data=product_specific_df.loc[:, "prompt_date"]
            )

            interpolated_product_curve_df = (
                time_series_interpolation.interpolate_on_time_series_df(
                    product_specific_df, "price", "interpolated_price"
                )
            )
            underlying_close_data = {}  # date: interpolated_price
            for row in interpolated_product_curve_df.itertuples():
                underlying_close_data[
                    row.index.strftime(r"%Y%m%d")
                ] = row.interpolated_price
            redis_pipeline.set(
                redis_key + redis_dev_key_append, ujson.dumps(underlying_close_data)
            )
        redis_pipeline.set(
            LME_FCP_RECENCY_KEY + redis_dev_key_append,
            most_recent_file_dt.strftime(r"%Y%m%d"),
        )
        redis_pipeline.execute()


def update_option_closing_prices_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_CLO_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.strptime(most_recent_file, r"%Y%m%d")
        except ValueError:
            pass
    with sqlalchemy.orm.Session(engine) as session:
        (
            most_recent_file_dt,
            _,
        ) = lme_staticdata_utils.update_lme_options_closing_price_data(
            session, most_recent_datetime=num_to_pull_or_dt
        )
        if most_recent_file_dt == datetime(1970, 1, 1):
            return
        session.commit()
        # currently we don't do anything with the options closing price data
        # in redis or database, just leaving it there for historical reasons.
        clo_file_date_str = most_recent_file_dt.strftime(r"%Y%m%d")
        pipeline = redis_conn.pipeline()
        pipeline.set(LEGACY_LME_CLO_RECENCY_KEY, clo_file_date_str)
        pipeline.set(LME_CLO_RECENCY_KEY, clo_file_date_str)
        pipeline.execute()
