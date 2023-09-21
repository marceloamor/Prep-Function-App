from prep.helpers import lme_staticdata_utils
from prep.helpers import lme_date_calc_funcs
from exceptions import ProductNotFound

from upedata.static_data import Exchange, Product

from dateutil import relativedelta
import sqlalchemy.orm
import sqlalchemy
import redis
import ujson

from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List
import logging
import os

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "1").lower() not in (
    "t",
    "true",
    "y",
    "yes",
    "1",
)
redis_dev_key_append = ":dev" if USE_DEV_KEYS else ""

LME_3M_DATE_KEYS = ujson.loads(
    os.getenv("LME_3M_DATE_LOCATIONS_REDIS", '["3m", "lme:3m_date"]')
)
LME_CASH_DATE_KEYS = ujson.loads(
    os.getenv("LME_CASH_DATE_LOCATIONS_REDIS", '["lme:cash_date"]')
)
LME_TOM_DATE_KEYS = ujson.loads(
    os.getenv("LME_TOM_DATE_LOCATIONS_REDIS", '["lme:tom_date"]')
)


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
