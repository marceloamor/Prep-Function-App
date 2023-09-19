from upedata.static_data import product, holiday
from prep.helpers import lme_staticdata_utils
from prep.helpers import lme_date_calc_funcs
from exceptions import ProductNotFound

from dateutil import relativedelta
import sqlalchemy.orm
import sqlalchemy
import requests
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
    redis_conn: redis.Redis, engine: sqlalchemy.Engine
):
    now_london_datetime = datetime.now(
        tz=ZoneInfo("Europe/London")
    ) + relativedelta.relativedelta(hours=6)
    if now_london_datetime.weekday >= 5:
        logging.info("No updating 3M date on weekends")
        return

    holiday_dates: List[date] = []

    with sqlalchemy.orm.Session(engine) as session:
        lme_copper_product = session.get(product.Product, "xlme-lcu-usd")
        if lme_copper_product is None:
            raise ProductNotFound("Unable to find `xlme-lcu-usd` in database")

        product_holidays = lme_copper_product.holidays
        holiday_dates = [holiday_obj.holiday_date for holiday_obj in product_holidays]

        lme_prompt_map = lme_date_calc_funcs.get_lme_prompt_map(holiday_dates)
        lme_3m_date = lme_date_calc_funcs.get_3m_datetime(
            now_london_datetime, lme_prompt_map
        )
        lme_cash_date = lme_date_calc_funcs.get_cash_datetime(
            now_london_datetime, product_holidays
        )
        lme_tom_date = lme_date_calc_funcs.get_tom_datetime(
            now_london_datetime, product_holidays
        )
        lme_weekly_datetimes = lme_date_calc_funcs.get_all_valid_weekly_prompts(
            now_london_datetime, lme_prompt_map
        )
        lme_monthly_datetimes = lme_date_calc_funcs.get_valid_monthly_prompts(
            now_london_datetime, forward_months=18
        )
        if lme_tom_date is None:
            future_expiries = set(
                [lme_cash_date, lme_3m_date]
                + lme_weekly_datetimes
                + lme_monthly_datetimes
            )
        else:
            future_expiries = set(
                [lme_tom_date, lme_cash_date, lme_3m_date]
                + lme_weekly_datetimes
                + lme_monthly_datetimes
            )

    redis_pipeline = redis_conn.pipeline()
    for key in LME_3M_DATE_KEYS:
        redis_pipeline.set(key + redis_dev_key_append, lme_3m_date.strftime(r"%Y%m%d"))
    for key in LME_CASH_DATE_KEYS:
        redis_pipeline.set(
            key + redis_dev_key_append, lme_cash_date.strftime(r"%Y%m%d")
        )
    for key in LME_TOM_DATE_KEYS:
        redis_pipeline.set(key + redis_dev_key_append, lme_tom_date.strftime(r"%Y%m%d"))

    redis_pipeline.execute()
