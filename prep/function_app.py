from prep.helpers import lme_staticdata_utils

from upedata.static_data import Exchange

from redis.backoff import ExponentialBackoff
import azure.functions as func
from redis.retry import Retry
import sqlalchemy.orm
import sqlalchemy
import redis

import os


app = func.FunctionApp()

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_KEY"),
    ssl=True,
    retry=Retry(
        ExponentialBackoff(),
        10,
    ),
    retry_on_timeout=True,
)

sqlalchemy_pg_url = sqlalchemy.URL(
    "postgresql+psycopg",
    os.getenv("DB_SERVER_USERNAME"),
    os.getenv("DB_SERVER_PASSWORD"),
    os.getenv("DB_SERVER_HOST"),
    int(os.getenv("DB_SERVER_PORT")),
    os.getenv("DB_SERVER_DATABASE"),
    query={},
)

pg_engine = sqlalchemy.create_engine(sqlalchemy_pg_url, echo=False)


@app.function_name(name="rjo_sftp_lme_overnight_poll")
# @app.schedule(schedule=)
def check_for_new_lme_overnight_files(timer: func.TimerRequest):
    pass


@app.function_name(name="lme_date_data_updater")
@app.schedule(schedule="32 1 * * MON-FRI", arg_name="timer", run_on_startup=True)
def update_lme_date_data(timer: func.TimerRequest):
    with sqlalchemy.orm.Session(pg_engine) as session:
        lme_exchange_obj = session.get(Exchange, "xlme")
        if lme_exchange_obj is None:
            raise ValueError("Unable to find LME exchange under symbol `xlme`")
        for product in lme_exchange_obj.products:
            lme_staticdata_utils.update_lme_product_static_data(product, session)
            # per product update logic for health keys could go here or elsewhere
