import paramiko.client
import pandas

from typing import Tuple, List
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import os


logger = logging.getLogger("prep.helpers.rjo_sftp_utils")


def get_rjo_ssh_client() -> paramiko.client.SSHClient:
    ssh_client = paramiko.client.SSHClient()
    ssh_client.load_host_keys("./prep/helpers/data_files/rjo_known_hosts")
    ssh_client.connect(
        hostname=os.getenv("RJO_SFTP_HOST"),
        port=int(os.getenv("RJO_SFTP_PORT")),
        username=os.getenv("RJO_SFTP_USER"),
        password=os.getenv("RJO_SFTP_PASS"),
    )
    logger.debug("Generated RJO SSH client")
    return ssh_client


def get_lme_overnight_data(
    base_file_name: str,
    fetch_most_recent_num=1,
) -> Tuple[List[datetime], List[pandas.DataFrame]]:
    """Fetches and sorts a list of datetimes and associated dataframes
    of LME overnight data files that are found in the RJO SFTP server.

    Return lists are sorted most recent first.

    :param base_file_name: The base name of the file, `INR`, `FCP`, and
    `CLO` are all examples.
    :type base_file_name: str
    :param fetch_most_recent_num: Number of files to fetch, most recent
    first, defaults to 1
    :type fetch_most_recent_num: int, optional
    :return: A tuple containing a list of datetimes and a list of the
    data contained in each of the files found associated with the given
    datetime
    :rtype: Tuple[List[datetime], List[pandas.DataFrame]]
    """
    file_datetimes: List[datetime] = []
    file_dfs: List[pandas.DataFrame] = []
    with get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/LMEPrices")
            sftp_files: List[Tuple[datetime, str]] = []
            for filename in rjo_sftp_client.listdir():
                try:
                    file_datetime = datetime.strptime(
                        f"%Y%m%d_{base_file_name}_r.csv", filename
                    )
                    sftp_files.append((file_datetime, filename))
                except ValueError:
                    pass
            sorted_sftp_files = sorted(
                sftp_files, key=lambda file_tuple: file_tuple[0], reverse=True
            )
            fetch_most_recent_num = (
                fetch_most_recent_num
                if fetch_most_recent_num < len(sorted_sftp_files)
                else len(sorted_sftp_files)
            )
            if fetch_most_recent_num > len(
                sorted_sftp_files
            ) or fetch_most_recent_num in (-1, 0):
                fetch_most_recent_num = len(sorted_sftp_files)

            for file_dt, filename in sorted_sftp_files[0:fetch_most_recent_num]:
                with rjo_sftp_client.open(filename) as sftp_file:
                    file_dataframe = pandas.read_csv(sftp_file, sep=",")
                    file_dataframe.columns = (
                        file_dataframe.columns.str.lower()
                        .str.strip()
                        .str.replace(" ", "_")
                    )
                    file_dfs.append(file_dataframe)
                    file_datetimes.append(file_dt)

    return file_datetimes, file_dfs
