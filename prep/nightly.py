from helpers.lme_date_calculation_functions import get_3m_date
from upedata.static_data import product, holiday
from exceptions import ProductNotFound

from dateutil import relativedelta
import sqlalchemy.orm
import sqlalchemy
import redis

from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import List
import logging


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

    lme_3m_date = get_3m_date(now_london_datetime.date(), holiday_dates)
