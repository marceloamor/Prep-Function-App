import json
import logging
import os
from datetime import datetime
from typing import List

import azure.functions as func
import redis
import sqlalchemy
import sqlalchemy.orm
import upedata.static_data as upestatic
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from upedata.static_data import Exchange, Option
from zoneinfo import ZoneInfo

import prep.nightly as nightly_funcs
from prep import handy_dandy_variables
from prep.lme import contract_db_gen, date_calc_funcs
from prep.data_ingestion import sol3_redis_ingestion
from prep.data_ingestion import sftp_file_ingestion

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
        send_usd_product_cache_update()
        send_eur_product_cache_update()


@app.function_name(name="rjo_sftp_update_fcp_data")
@app.schedule(schedule="15 11/30 21-23,0-10 * * MON-FRI", arg_name="timer")
def update_fcp_data(timer: func.TimerRequest):
    logging.info("Updating FCP data")
    fcp_updated = nightly_funcs.update_future_closing_prices_from_lme(
        redis_conn, pg_engine, first_run=True
    )
    if fcp_updated:
        with pg_engine.connect() as connection:
            connection.execute(sqlalchemy.text("CALL refresh_most_recent_fcps()"))
            logging.info("Refreshed most recent future close price materialised view")
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
    with sqlalchemy.orm.Session(pg_engine) as session:
        contract_db_gen.update_lme_static_data(session)
        session.commit()
    logging.info("Completed LME static data update")


@app.function_name(name="update_lme_important_dates")
@app.schedule(
    schedule="32 5 20 * * SUN-THU",
    arg_name="timer",
    use_monitor=True,
)
def update_lme_important_dates(timer: func.TimerRequest):
    logging.info("Starting LME static data update job")
    with sqlalchemy.orm.Session(pg_engine) as session:
        lme_ali_orm = session.get(upestatic.Product, "xlme-lad-usd")
        if lme_ali_orm is None:
            raise ValueError("Unable to find xlme-lad-usd in database products")
        non_prompts = [holiday.holiday_date for holiday in lme_ali_orm.holidays]
        forward_months = 18
        current_datetime = datetime.now(tz=ZoneInfo("Europe/London"))
        prompt_curve = date_calc_funcs.populate_primary_curve_datetimes(
            non_prompts,
            lme_ali_orm.holidays,
            forward_months,
            _current_datetime=current_datetime,
        )

    lme_3m_datetime = prompt_curve.three_month
    lme_cash_datetime = prompt_curve.cash
    lme_tom_datetime = prompt_curve.tom

    redis_pipeline = redis_conn.pipeline()
    for key in nightly_funcs.LME_3M_DATE_KEYS:
        redis_pipeline.set(
            key + nightly_funcs.redis_dev_key_append,
            lme_3m_datetime.strftime(r"%Y%m%d"),
        )
        logging.info(
            "Set 3M redis key `%s` to `%s",
            key + nightly_funcs.redis_dev_key_append,
            lme_3m_datetime.strftime(r"%Y%m%d"),
        )
    for key in nightly_funcs.LME_CASH_DATE_KEYS:
        redis_pipeline.set(
            key + nightly_funcs.redis_dev_key_append,
            lme_cash_datetime.strftime(r"%Y%m%d"),
        )
        logging.info(
            "Set CASH redis key `%s` to `%s",
            key + nightly_funcs.redis_dev_key_append,
            lme_cash_datetime.strftime(r"%Y%m%d"),
        )
    for key in nightly_funcs.LME_TOM_DATE_KEYS:
        if lme_tom_datetime is not None:
            redis_pipeline.set(
                key + nightly_funcs.redis_dev_key_append,
                lme_tom_datetime.strftime(r"%Y%m%d"),
            )
            logging.info(
                "Set TOM redis key `%s` to `%s",
                key + nightly_funcs.redis_dev_key_append,
                lme_tom_datetime.strftime(r"%Y%m%d"),
            )
        else:
            # In the case where there isn't a TOM date (i.e. double cash days)
            # we want to push no value to Redis for the TOM date so delete it.
            redis_pipeline.delete(key + nightly_funcs.redis_dev_key_append)
            logging.info(
                "Cleared TOM redis key `%s` due to double cash day",
                key + nightly_funcs.redis_dev_key_append,
            )

    redis_pipeline.execute()


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


def send_ice_cache_updates():
    logging.info("Sending XICE cache update command on redis")
    with sqlalchemy.orm.Session(pg_engine) as session:
        xice_options = get_options_from_exchange_symbol_static_data(session, "xice")
        send_static_data_update_for_product_ids(REDIS_COMPUTE_CHANNEL, xice_options)


def send_euronext_cache_update():
    logging.info("Sending XEXT cache update command on redis")
    with sqlalchemy.orm.Session(pg_engine) as session:
        xext_options = get_options_from_exchange_symbol_static_data(session, "xext")
        send_static_data_update_for_product_ids(REDIS_COMPUTE_CHANNEL, xext_options)


def send_usd_product_cache_update():
    logging.info("Sending USD product update command on redis")
    send_lme_cache_update()
    send_ice_cache_updates()


def send_eur_product_cache_update():
    logging.info("Sending EUR product update command on redis")
    send_euronext_cache_update()


# ingestion of sol3 redis data and pushing to postgres
@app.function_name(name="cme_redis_data_pusher")
@app.schedule(
    schedule="10 10 22 * * 1-5",
    arg_name="timer",
    use_monitor=True,
)
def redis_data_pusher(timer: func.TimerRequest):
    logging.info("Pulling redis keys with pattern `sol3:XCME*`")
    status = sol3_redis_ingestion.push_redis_data_to_postgres(redis_conn, pg_engine)
    logging.info("Completed pulling sol3 xcme data with status `%s`", status)


# ingestion of rjo sftp files and saving to upe sftp server
@app.function_name(name="daily_sftp_file_saver")
@app.schedule(
    schedule="11 11 11 * * 1-5",
    arg_name="timer",
    use_monitor=True,
)
def daily_sftp_file_saver(timer: func.TimerRequest):
    # download the most recent file from RJO SFTP
    logging.info("Connecting to the RJO SFTP server")
    daily_files_to_fetch = [
        "UPETRADING_csvnmny_nmny_%Y%m%d.csv",
        "UPETRADING_csvnpos_npos_%Y%m%d.csv",
        "UPETRADING_csvth1_dth1_%Y%m%d.csv",
        "UPETRADING_statement_dstm_%Y%m%d.pdf",
    ]

    files = sftp_file_ingestion.download_file_from_rjo_sftp(daily_files_to_fetch)
    if len(files) == 0: 
        return "No files found in RJO SFTP"
    logging.info(f"Files successfully downloaded from RJO SFTP: {files}")
    # post the file to UPE SFTP
    sftp_file_ingestion.post_file_to_upe_sftp(files)
    logging.info(f"Files have been successfully posted to UPE SFTP: {files}")

    # clear the temp_assets folder
    sftp_file_ingestion.clear_temp_assets_after_upload()
    logging.info("Temp assets folder has been cleared")


# ingestion of rjo sftp files and saving to upe sftp server
@app.function_name(name="monthly_sftp_file_saver")
@app.schedule(
    #schedule="11 11 11 * * 1-5",
    # on the 5th day of the month at 11:22:11
    schedule="11 22 11 05 *",
    arg_name="timer",
    use_monitor=True,
)
def monthly_sftp_file_saver(timer: func.TimerRequest):
    # download the most recent file from RJO SFTP
    logging.info("Connecting to the RJO SFTP server")
    monthly_files_to_fetch = [
        "UPETRADING_statement_mstm_%Y%m%d.pdf",
        "UPETRADING_monthlytrans_mtrn_%Y%m%d.csv",
    ]

    files = sftp_file_ingestion.download_file_from_rjo_sftp(monthly_files_to_fetch)
    if len(files) == 0: 
        return "No files found in RJO SFTP"
    logging.info(f"Files successfully downloaded from RJO SFTP: {files}")
    # post the file to UPE SFTP
    sftp_file_ingestion.post_file_to_upe_sftp(files)
    logging.info(f"Files have been successfully posted to UPE SFTP: {files}")

    # clear the temp_assets folder
    sftp_file_ingestion.clear_temp_assets_after_upload()
    logging.info("Temp assets folder has been cleared")