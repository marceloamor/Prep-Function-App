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
    _now_dt=datetime.now(tz=ZoneInfo("Europe/London")),
) -> Tuple[List[datetime], List[pandas.DataFrame]]:
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
                sftp_files,
                key=lambda file_tuple: (_now_dt - file_tuple[0]).total_seconds(),
                reverse=True,
            )
            fetch_most_recent_num = (
                fetch_most_recent_num
                if fetch_most_recent_num < len(sorted_sftp_files)
                else len(sorted_sftp_files)
            )
            for file_dt, filename in sorted_sftp_files[0:fetch_most_recent_num]:
                with rjo_sftp_client.open(filename) as sftp_file:
                    file_dfs.append(pandas.read_csv(sftp_file, sep=","))
                    file_datetimes.append(file_dt)

    return file_datetimes, file_dfs
