import prep.nightly as nightly_funcs

from redis.backoff import ExponentialBackoff
import azure.functions as func
from redis.retry import Retry
import sqlalchemy.orm
import sqlalchemy
import redis

import logging
import os


app = func.FunctionApp()

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST"),  # type: ignore
    port=int(os.getenv("REDIS_PORT", "6379")),
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
    int(os.getenv("DB_SERVER_PORT", "5432")),
    os.getenv("DB_SERVER_DATABASE"),
    query={},  # type: ignore
)

pg_engine = sqlalchemy.create_engine(sqlalchemy_pg_url, echo=False)


# The first runs are all marked as true because there's no really safe way to store state
# in these applications without them sometimes shitting the bed, at least with this and
# downstream changes they should be only pulling files they think they need based on the
# underlying redis health keys


# @app.function_name(name="rjo_sftp_lme_overnight_poll")
# @app.schedule(schedule="4/30 2-12 * * TUE-SAT", arg_name="timer", run_on_startup=True)
# def check_for_new_lme_overnight_files(timer: func.TimerRequest):
#     logging.info("Checking for updated LME overnight files")
#     logging.info("Updating INR data")
#     nightly_funcs.update_currency_interest_curves_from_lme(
#         redis_conn, pg_engine, first_run=True
#     )
#     logging.info("Updating FCP data")
#     nightly_funcs.update_future_closing_prices_from_lme(
#         redis_conn, pg_engine, first_run=True
#     )
#     logging.info("Updating CLO data")
#     nightly_funcs.update_option_closing_prices_from_lme(
#         redis_conn, pg_engine, first_run=True
#     )


@app.function_name(name="lme_date_data_updater")
@app.schedule(
    schedule="32 1 1 * * *", arg_name="timer", run_on_startup=True, use_monitor=True
)
def update_lme_date_data(timer: func.TimerRequest):
    logging.info("Starting LME static data update job")
    nightly_funcs.update_lme_relative_forward_dates(
        redis_conn, pg_engine, first_run=True
    )
    logging.info("Completed LME static data update")
