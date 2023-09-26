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

    with sqlalchemy.orm.Session(engine) as session:
        (
            most_recent_rate_datetime,
            updated_currencies,
        ) = lme_staticdata_utils.update_lme_interest_rate_static_data(
            session, first_run=first_run
        )
        select_most_recent_inr_curve = (
            sqlalchemy.select(InterestRate.to_date, InterestRate.continuous_rate)
            .where(InterestRate.source == "LME")
            .where(InterestRate.published_date == most_recent_rate_datetime.date())
        )
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
    for updated_currency_iso in updated_currencies:
        redis_pipeline.set(
            f"{updated_currency_iso.upper()}Rate{redis_dev_key_append}",
            ujson.dumps(rate_curve_data[updated_currency_iso.upper()]["legacy"]),
        )
        redis_pipeline.set(
            UPDATED_CURRENCY_TO_KEY[updated_currency_iso.upper()]
            + redis_dev_key_append,
            most_recent_rate_datetime.strftime(r"%Y%m%d"),
        )
    redis_pipeline.execute()
