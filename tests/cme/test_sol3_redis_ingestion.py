import logging
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import pytest
import json

# from dateutil import relativedelta
from upedata.static_data import Holiday
from zoneinfo import ZoneInfo

from prep.cme import sol3_redis_ingestion


# functions to test:
# filter_for_valid_redis_keys
# process_CME_redis_data
# push_redis_data_to_postgres


# no need to be parameterised, filters a list of arbitrary length
def test_filter_for_valid_redis_keys():
    current_date = datetime.now()
    past_date = (current_date - relativedelta(years=1)).strftime("%Y-%m")
    future_date_1 = (current_date + relativedelta(years=1)).strftime("%Y-%m")
    future_date_2 = (current_date + relativedelta(years=2)).strftime("%Y-%m")
    future_date_3 = (current_date + relativedelta(years=3)).strftime("%Y-%m")

    strings = [
        f"sol3:XCME:HXE-{current_date.strftime('%Y-%m')}",
        f"sol3:XCME:ABC-{current_date.strftime('%Y-%m')}",
        f"sol3:XCME:HXE-{past_date}",
        f"sol3:XCME:HXE-{future_date_1}",
        f"sol3:XCME:H5M-{future_date_2}",
        f"sol3:XCME:AX-{future_date_3}",

    ]

    expected_result = [
        f"sol3:XCME:HXE-{current_date.strftime('%Y-%m')}",
        f"sol3:XCME:HXE-{future_date_1}",
        f"sol3:XCME:H5M-{future_date_2}",
        f"sol3:XCME:AX-{future_date_3}",
    ]

    assert sol3_redis_ingestion.filter_for_valid_redis_keys(strings) == expected_result



@pytest.mark.parametrize(
    ["key", "raw_data", "expected_result"],
    [
        (  # base test case, requiring some 0 padding due to missing values
            "sol3:XCME:HXE-2025-06",
            json.dumps(
                {
                    "100.0": {"v": 0.5, "dvds": 0.1, "d2vd2s": 0.2},
                    "200.0": {"v": 0.6, "dvds": 0.2},
                    "300": {"v": 0.7, "dvds": 0.3, "d2vd2s": 0.4},
                }
            ),
            {
                "date_ingested": datetime.now().date(),
                "instrument_symbol": "HXE-2025-06",
                "strikes": [100.0, 200.0, 300.0],
                "volatilities": [0.5, 0.6, 0.7],
                "dvds": [0.1, 0.2, 0.3],
                "d2vd2s": [0.2, 0.0, 0.4],
            },
        ),
        (  # missing vols case
            "sol3:XCME:HXE-2025-06",
            json.dumps(
                {
                    "100.0": {"v": 0, "dvds": 0.1, "d2vd2s": 0.2},
                    "200.0": {"v": 0, "dvds": 0.2},
                    "300.0": {"v": 0, "dvds": 0.3, "d2vd2s": 0.4},
                }
            ),
            None,
        ),
        (  # missing data case
            "sol3:XCME:HXE-2025-06",
            None,
            None,
        ),
    ],
)
def test_process_CME_redis_data(key, raw_data, expected_result):
    assert sol3_redis_ingestion.process_CME_redis_data(key, raw_data) == expected_result
