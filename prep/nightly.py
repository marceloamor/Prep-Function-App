import logging
import os
from datetime import datetime

import pandas as pd
import redis
import sqlalchemy
import sqlalchemy.orm
import ujson
from dateutil import relativedelta
from upedata.static_data import Currency

from prep import handy_dandy_variables
from prep.helpers import lme_staticdata_utils, time_series_interpolation
from prep.lme import contract_db_gen

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
LEGACY_LME_EXR_RECENCY_KEY = os.getenv("LEGACY_LME_EXR_RECENCY_KEY", "EXR_update")
LME_INR_RECENCY_KEY = os.getenv("LME_INR_RECENCY_KEY", "prep:health:lme:inr")
LME_FCP_RECENCY_KEY = os.getenv("LME_FCP_RECENCY_KEY", "prep:health:lme:fcp")
LME_CLO_RECENCY_KEY = os.getenv("LME_CLO_RECENCY_KEY", "prep:health:lme:clo")
LME_EXR_RECENCY_KEY = os.getenv("LME_EXR_RECENCY_KEY", "prep:health:lme:exr")

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
        contract_db_gen.LME_PRODUCT_NAMES,
        contract_db_gen.GEORGIA_LME_PRODUCT_NAMES_BASE,
    )
}


def update_exchange_rate_curves_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine
):
    lme_last_exr_dt_iso = redis_conn.get(LME_EXR_RECENCY_KEY + redis_dev_key_append)
    if lme_last_exr_dt_iso is None:
        files_to_fetch = -1
    else:
        files_to_fetch = datetime.fromisoformat(
            lme_last_exr_dt_iso
        ) + relativedelta.relativedelta(days=1)
    with sqlalchemy.orm.Session(engine) as session:
        currency_iso_symbols = (
            session.execute(sqlalchemy.select(Currency.iso_symbol)).scalars().all()
        )
        most_recent_datetime = lme_staticdata_utils.update_lme_exchange_rate_data(
            session, files_to_fetch, set(currency_iso_symbols)
        )
        session.commit()
    pipeline = redis_conn.pipeline()
    pipeline.set(
        LME_EXR_RECENCY_KEY + redis_dev_key_append, most_recent_datetime.isoformat()
    )
    pipeline.set(
        LEGACY_LME_EXR_RECENCY_KEY + redis_dev_key_append,
        most_recent_datetime.strftime(r"%Y%m%d"),
    )
    pipeline.execute()


def update_currency_interest_curves_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
) -> bool:
    rate_curve_data = {
        currency_iso_sym: {"legacy": {}, "new": {}}
        for currency_iso_sym in list(UPDATED_CURRENCY_TO_KEY.keys())
    }

    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_INR_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.fromisoformat(
                most_recent_file
            ) + relativedelta.relativedelta(days=1)
        except ValueError:
            pass
    with sqlalchemy.orm.Session(engine) as session:
        (
            most_recent_rate_datetime,
            updated_currencies,
        ) = lme_staticdata_utils.update_lme_interest_rate_static_data(
            session, most_recent_datetime=num_to_pull_or_dt
        )
        # selects the most recent set of interest rates published for specific
        # currency and then returns the date they're forward to and the cont_rate
        select_most_recent_inr_curve_stmt = sqlalchemy.text(
            """
                SELECT DISTINCT ON (to_date, currency_symbol, "source")
                    to_date, continuous_rate
                    FROM interest_rates
                WHERE "source" = 'LME' AND to_date >= CURRENT_DATE - 1 AND currency_symbol = :currency_symbol
                ORDER BY to_date, currency_symbol, "source", published_date DESC
            """
        )
        session.commit()
        for curr_iso_sym, rate_data in rate_curve_data.items():
            interest_rates = session.execute(
                select_most_recent_inr_curve_stmt,
                {"currency_symbol": curr_iso_sym.lower()},
            )
            interest_rate_df = pd.DataFrame.from_records(
                interest_rates,
                columns=["to_date", "continuous_rate"],
            )
            interest_rate_df.loc[:, "continuous_rate"] = pd.to_numeric(
                interest_rate_df.loc[:, "continuous_rate"], downcast="float"
            )
            interest_rate_df.index = pd.DatetimeIndex(
                data=pd.to_datetime(interest_rate_df["to_date"].to_list())
            )

            interped_interest_rate_df = (
                time_series_interpolation.interpolate_on_time_series_df(
                    interest_rate_df,
                    "continuous_rate",
                    "interp_cont_rate",
                )
            )
            for interest_rate_row_data in interped_interest_rate_df.itertuples():
                rate_data["legacy"][
                    interest_rate_row_data.Index.strftime(r"%Y%m%d")  # type: ignore
                ] = {"Interest Rate": interest_rate_row_data.interp_cont_rate}
                rate_data["new"][interest_rate_row_data.Index.strftime(r"%Y%m%d")] = (  # type: ignore
                    interest_rate_row_data.interp_cont_rate
                )
        session.commit()

    redis_pipeline = redis_conn.pipeline()
    most_recent_dt_Ymd = most_recent_rate_datetime.strftime(r"%Y%m%d")
    most_recent_dt_iso = most_recent_rate_datetime.isoformat()
    for updated_currency_iso in updated_currencies:
        redis_pipeline.set(
            f"{updated_currency_iso.upper()}Rate{redis_dev_key_append}",
            ujson.dumps(rate_curve_data[updated_currency_iso.upper()]["legacy"]),
        )
        redis_pipeline.set(
            f"prep:cont_interest_rate:{updated_currency_iso.lower()}{redis_dev_key_append}",
            ujson.dumps(rate_curve_data[updated_currency_iso.upper()]["new"]),
        )
        redis_pipeline.set(
            UPDATED_CURRENCY_TO_KEY[updated_currency_iso.upper()]
            + redis_dev_key_append,
            most_recent_dt_iso,
        )
        logging.info("Updated `INR` data for %s", updated_currency_iso.upper())
    redis_pipeline.set(
        LEGACY_LME_INR_RECENCY_KEY + redis_dev_key_append, most_recent_dt_Ymd
    )
    redis_pipeline.set(LME_INR_RECENCY_KEY + redis_dev_key_append, most_recent_dt_iso)
    redis_pipeline.execute()

    return most_recent_file != most_recent_dt_iso


def update_future_closing_prices_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
) -> bool:
    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_FCP_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.fromisoformat(
                most_recent_file
            ) + relativedelta.relativedelta(days=1)
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
            return False
        session.commit()
        most_recent_file_df = most_recent_file_df[
            (most_recent_file_df["currency"] == "USD")
            & (most_recent_file_df["price_type"] == "FC")
        ]
        most_recent_file_df.loc[:, "prompt_date"] = most_recent_file_df.loc[
            :, "forward_date"
        ]  # .apply(lambda forward_date: datetime.strptime(str(forward_date), r"%Y%m%d"))

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
                underlying_close_data[row.Index.to_pydatetime().strftime(r"%Y%m%d")] = (  # type: ignore
                    round(row.interpolated_price, 2)  # type: ignore
                )
            redis_pipeline.set(
                redis_key + redis_dev_key_append, ujson.dumps(underlying_close_data)
            )
        redis_pipeline.set(
            LME_FCP_RECENCY_KEY + redis_dev_key_append,
            most_recent_file_dt.isoformat(),
        )
        redis_pipeline.execute()

    return most_recent_file != most_recent_file_dt.isoformat()


def update_option_closing_prices_from_lme(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    num_to_pull_or_dt = -1 if first_run else 1
    most_recent_file = redis_conn.get(LME_CLO_RECENCY_KEY + redis_dev_key_append)
    if most_recent_file is not None:
        try:
            num_to_pull_or_dt = datetime.fromisoformat(
                most_recent_file
            ) + relativedelta.relativedelta(days=1)
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
        most_recent_dt_iso = most_recent_file_dt.isoformat()
        pipeline = redis_conn.pipeline()
        pipeline.set(
            LEGACY_LME_CLO_RECENCY_KEY + redis_dev_key_append, clo_file_date_str
        )
        pipeline.set(LME_CLO_RECENCY_KEY + redis_dev_key_append, most_recent_dt_iso)
        pipeline.execute()
