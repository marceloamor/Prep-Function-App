import re, json
from typing import List
from datetime import datetime

import redis
import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.dialects.postgresql import insert as pg_insert

from prep import handy_dandy_variables


redis_dev_key_append = handy_dandy_variables.redis_key_append


# nightly function to scan for sol3:XCME keys, filter for expired keys, pull remaining and publish to db
def push_redis_data_to_postgres(
    redis_conn: redis.Redis, engine: sqlalchemy.Engine, first_run=False
):
    # first scan pull all keys matching pattern
    cme_keys = [key for key in redis_conn.scan_iter("sol3:XCME*")]

    # filter those for expired keys
    active_cme_keys = filter_for_valid_redis_keys(cme_keys)

    entries_to_publish = []

    for key in active_cme_keys:
        raw_data = redis_conn.get(key)
        if raw_data is not None:
            data = process_CME_redis_data(key, raw_data)
            if data is not None:
                entries_to_publish.append(data)

    cme_vol_curves_table = sqlalchemy.Table(
        "cme_vol_curves", sqlalchemy.MetaData(), autoload_with=engine
    )

    # send to db with upsert
    with engine.connect() as connection:
        stmt = pg_insert(cme_vol_curves_table).values(entries_to_publish)

        update_dict = {col: stmt.excluded[col] for col in entries_to_publish[0].keys()}

        stmt = stmt.on_conflict_do_update(
            index_elements=["date_ingested", "instrument_symbol"], set_=update_dict
        )

        connection.execute(stmt)
        connection.commit()
    return "Data pushed to Postgres successfully!"


# function to filter out expired and irrelevant keys
def filter_for_valid_redis_keys(strings: List[str]) -> List[str]:
    cme_symbols = [
        "AX",
        "HXE",
        "H1W",
        "H1E",
        "H1M",
        "H2E",
        "H2M",
        "H2W",
        "H3E",
        "H3M",
        "H3W",
        "H4E",
        "H4M",
        "H4W",
        "H1E",
        "H1M",
        "H2E",
        "H2M",
        "H2W",
        "H3E",
        "H3M",
        "H3W",
        "H4E",
        "H4M",
        "H4W",
        "H5E",
        "H5M",
        "H5W",
    ]
    current_date = datetime.now()

    # regex to match the date pattern on sol3 cme keys (format: sol3:XCME:HXE-2021-01)
    date_pattern = re.compile(r"-(\d{4}-\d{2})$")

    filtered_strings = []

    for string in strings:
        match = date_pattern.search(string)
        if match:
            date_str = match.group(1)
            # convert the extracted date string to a datetime object
            date_obj = datetime.strptime(date_str, "%Y-%m")
            # compare with the current date
            if date_obj.year > current_date.year or (
                date_obj.year == current_date.year
                and date_obj.month >= current_date.month
            ):
                # now check if instrument is of interest to us, dictacted by the cme_symbols list
                instrument_symbol = string.split(":")[2].split("-")[0]
                if instrument_symbol in cme_symbols:
                    filtered_strings.append(string)
    return filtered_strings


# function to format raw data from redis for database entry
def process_CME_redis_data(key: str, raw_data):
    if raw_data is None:
        return None

    # process key into instrument name
    instrument_symbol = key.split(":")[2]

    data = json.loads(raw_data)
    strikes = []
    volatilities = []
    dvds_list = []
    d2vd2s_list = []

    date_ingested = datetime.now().date()

    for strike, values in data.items():
        strikes.append(float(strike))
        volatilities.append(float(values.get("v", 0)))
        dvds_list.append(float(values.get("dvds", 0)))
        d2vd2s_list.append(float(values.get("d2vd2s", 0)))

    # handle valid key, empty data case
    if sum(volatilities) == 0:
        return None

    return {
        "date_ingested": date_ingested,
        "instrument_symbol": instrument_symbol,
        "strikes": strikes,
        "volatilities": volatilities,
        "dvds": dvds_list,
        "d2vd2s": d2vd2s_list,
    }
