import os
import json
import logging
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
import sqlalchemy.orm
from dateutil.relativedelta import WE, relativedelta
from sqlalchemy.dialects.postgresql import insert as pg_insert
from zoneinfo import ZoneInfo

from prep.helpers import rjo_sftp_utils



"""
going to need a
- list of file names to look out for 
- function to go into RJO sftp and pull files
- function to push files into our local sftp


"""

daily_files_to_fetch = [
    "UPETRADING_csvnmny_nmny_%Y%m%d.csv",
    "UPETRADING_csvnpos_npos_%Y%m%d.csv",
    "UPETRADING_csvth1_dth1_%Y%m%d.csv",
    "UPETRADING_statement_dstm_%Y%m%d.pdf",
]

monthly_files_to_fetch = [
    "UPETRADING_statement_mstm_%Y%m%d.pdf",
    "UPETRADING_monthlytrans_mtrn_%Y%m%d.csv",
]


#
def download_file_from_rjo_sftp(filename_pattern:str ): 
    with rjo_sftp_utils.get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/OvernightReports")
            sftp_files: List[Tuple[datetime, str]] = []
            #filename_pattern = f"{file_name}_%Y%m%d.csv"
            filename_pattern = "UPETRADING_csvnmny_nmny_%Y%m%d.csv"
            for filename in rjo_sftp_client.listdir():
                try:
                    file_date = datetime.strptime(filename, filename_pattern)
                    sftp_files.append((file_date, filename))
                except ValueError:
                    continue

            sftp_files.sort(key=lambda x: x[0], reverse=True)
            most_recent_file = sftp_files[0][1]
            if len(sftp_files) == 0:
                logging.warning(f"Found no recent enough files with basename {filename_pattern} in RJO SFTP")
                return None
            # download the most recent file into the prep/data_ingestion/temp_assets folder
            try:
                rjo_sftp_client.get(sftp_files[0][1], f"prep/data_ingestion/temp_assets/{most_recent_file}")
            except FileNotFoundError:
                logging.error(f"File {most_recent_file} not found in RJO SFTP")
                return None

            return sftp_files[0][1]
        


def post_file_to_upe_sftp(file_name:str, file_path:str):
    with rjo_sftp_utils.get_upe_ssh_client() as upe_ssh:
        with upe_ssh.open_sftp() as upe_sftp_client:
            upe_sftp_client.chdir("/rjo_file_backup")
            upe_sftp_client.put(file_path, file_name)
            logging.info(f"File {file_name} has been successfully posted to UPE SFTP")
            


def clear_temp_assets_after_upload():
    # get list of files in the temp_assets folder
    files = os.listdir("prep/data_ingestion/temp_assets")
    # delete each file
    for file in files:
        os.remove(f"prep/data_ingestion/temp_assets/{file}")


print("hello world")
# not working due to some issue with the rjo_sftp_utils host key verification
download_file_from_rjo_sftp("UPETRADING_csvnmny_nmny_%Y%m%d.csv")