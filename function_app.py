import json
import logging
import os
from typing import List

import azure.functions as func
import redis
import sqlalchemy
import sqlalchemy.orm
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from upedata.static_data import Exchange, Option

import prep.nightly as nightly_funcs
from prep import handy_dandy_variables

app = func.FunctionApp()

REDIS_COMPUTE_CHANNEL = (
    os.getenv("REDIS_COMPUTE_CHANNEL", "v2:compute")
    + handy_dandy_variables.redis_key_append
)

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
    decode_responses=True,
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


@app.function_name(name="rjo_sftp_update_inr_data")
@app.schedule(schedule="15 4/30 21-23,0-10 * * MON-FRI", arg_name="timer")
def update_inr_data(timer: func.TimerRequest):
    logging.info("Updating INR data")
    inr_updated = nightly_funcs.update_currency_interest_curves_from_lme(
        redis_conn, pg_engine, first_run=True
    )
    if inr_updated:
        send_lme_cache_update()
        send_euronext_cache_update()


@app.function_name(name="rjo_sftp_update_fcp_data")
@app.schedule(schedule="15 11/30 21-23,0-10 * * MON-FRI", arg_name="timer")
def update_fcp_data(timer: func.TimerRequest):
    logging.info("Updating FCP data")
    fcp_updated = nightly_funcs.update_future_closing_prices_from_lme(
        redis_conn, pg_engine, first_run=True
    )
    if fcp_updated:
        with pg_engine.connect() as connection:
            connection.execute(
                sqlalchemy.text("CALL refresh_materialised_view(most_recent_fcps)")
            )
        send_lme_cache_update()


@app.function_name(name="rjo_sftp_update_clo_data")
@app.schedule(schedule="15 21/30 21-23,0-10 * * MON-FRI", arg_name="timer")
def update_clo_data(timer: func.TimerRequest):
    logging.info("Updating CLO data")
    nightly_funcs.update_option_closing_prices_from_lme(
        redis_conn, pg_engine, first_run=True
    )


@app.function_name(name="rjo_sftp_update_exr_data")
@app.schedule(schedule="15 29/30 21-23,0-10 * * MON-FRI", arg_name="timer")
def update_exr_data(timer: func.TimerRequest):
    logging.info("Updating EXR data")
    nightly_funcs.update_exchange_rate_curves_from_lme(redis_conn, pg_engine)


@app.function_name(name="lme_date_data_updater")
@app.schedule(
    schedule="32 1 20 * * SUN-THU",
    arg_name="timer",
    use_monitor=True,
)
def update_lme_date_data(timer: func.TimerRequest):
    logging.info("Starting LME static data update job")
    nightly_funcs.update_lme_relative_forward_dates(
        redis_conn,
        pg_engine,
        first_run=True,
        # placeholder_dt=datetime(2023, 1, 1, tzinfo=ZoneInfo("Europe/London")),
    )
    logging.info("Completed LME static data update")


def send_static_data_update_for_product_ids(
    channel_key: str, options_to_update: List[Option]
):
    """Send a list of option symbols down the given pubsub channel to force updating
    of the attached option engine cache

    :param channel_key: Pubsub channel identifier key
    :type channel_key: str
    :param options_to_update: List of options to have symbols sent down, only the first will
    have the `staticdata` marker attached which causes a refresh of all cached static data
    information.
    :type options_to_update: List[Option]
    """
    product_symbols = set()
    for option_obj in options_to_update:
        product_symbols.add(option_obj.product_symbol)
    redis_conn.publish(
        REDIS_COMPUTE_CHANNEL,
        json.dumps({"type": "staticdata", "product_symbols": list(product_symbols)}),
    )
    logging.info(
        "Sent %s product symbol updates on channel: `%s`",
        len(product_symbols),
        channel_key,
    )


def get_options_from_exchange_symbol_static_data(
    sqla_session: sqlalchemy.orm.Session, exchange_symbol: str
) -> List[Option]:
    # this sort of pattern is fine in batch jobs but for stuff running
    # regularly during the trading day would be best packaged into a single
    # text query
    exchange = sqla_session.get(Exchange, exchange_symbol)
    if exchange is None:
        raise ValueError(f"Exchange with symbol `{exchange_symbol} was not found")

    options: List[Option] = []
    for product_obj in exchange.products:
        options.extend(product_obj.options)

    return options


def send_lme_cache_update():
    logging.info("Sending LME cache update command on redis")
    # this and the euronext one are the way they are because of a design change
    # in option engine, a smarter way to do this would involve providing
    # product symbols directly to the send_static_data... function
    with sqlalchemy.orm.Session(pg_engine) as session:
        lme_options = get_options_from_exchange_symbol_static_data(session, "xlme")
        send_static_data_update_for_product_ids(REDIS_COMPUTE_CHANNEL, lme_options)


def send_euronext_cache_update():
    logging.info("Sending XEXT cache update command on redis")
    with sqlalchemy.orm.Session(pg_engine) as session:
        xext_options = get_options_from_exchange_symbol_static_data(session, "xext")
        send_static_data_update_for_product_ids(REDIS_COMPUTE_CHANNEL, xext_options)
