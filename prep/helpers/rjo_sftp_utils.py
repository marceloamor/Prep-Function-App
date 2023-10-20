import paramiko.client
import pandas

from typing import Tuple, List, Union, Optional
from datetime import datetime
import logging
import os


def get_rjo_ssh_client() -> paramiko.client.SSHClient:
    ssh_client = paramiko.client.SSHClient()
    ssh_client.load_host_keys("./prep/helpers/data_files/rjo_known_hosts")
    rjo_sftp_host = os.getenv("RJO_SFTP_HOST")
    rjo_sftp_port = os.getenv("RJO_SFTP_PORT")
    assert rjo_sftp_host is not None, "RJO_SFTP_HOST wasn't provided"
    assert rjo_sftp_port is not None, "RJO_SFTP_PORT wasn't provided"
    ssh_client.connect(
        hostname=rjo_sftp_host,
        port=int(rjo_sftp_port),
        username=os.getenv("RJO_SFTP_USER"),
        password=os.getenv("RJO_SFTP_PASS"),
    )
    logging.debug("Generated RJO SSH client")
    return ssh_client


def get_lme_overnight_data(
    base_file_name: str,
    num_recent_or_since_dt: Union[int, datetime],
    date_cols_to_parse: Optional[List[str]] = [],
) -> Tuple[List[datetime], List[pandas.DataFrame]]:
    """Fetches and sorts a list of datetimes and associated dataframes
    of LME overnight data files that are found in the RJO SFTP server.

    Return lists are sorted most recent first.

    :param base_file_name: The base name of the file, `INR`, `FCP`, and `CLO` are all examples.
    :type base_file_name: str
    :param num_recent_or_since_dt: Number of files to count back (n <= 0 -> all files),
    or datetime in which case files with a datetime more recent than it will be pulled
    :type num_recent_or_since_dt: Union[int, datetime]
    :return: A tuple containing a list of datetimes and a list of the
        data contained in each of the files found associated with the given
        datetime
    :rtype: Tuple[List[datetime], List[pandas.DataFrame]]
    """
    file_datetimes: List[datetime] = []
    file_dfs: List[pandas.DataFrame] = []
    logging.info(
        "Searching for `%s` LME files, either dated after or for total count of: %s",
        base_file_name,
        num_recent_or_since_dt,
    )
    with get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/LMEPrices")
            sftp_files: List[Tuple[datetime, str]] = []
            filename_pattern = f"%Y%m%d_{base_file_name}_r.csv"
            for filename in rjo_sftp_client.listdir():
                try:
                    file_datetime = datetime.strptime(filename, filename_pattern)
                    sftp_files.append((file_datetime, filename))
                except ValueError:
                    pass
            sorted_sftp_files = sorted(
                sftp_files, key=lambda file_tuple: file_tuple[0], reverse=True
            )
            if isinstance(num_recent_or_since_dt, int):
                num_recent_or_since_dt = (
                    num_recent_or_since_dt
                    if num_recent_or_since_dt < len(sorted_sftp_files)
                    else len(sorted_sftp_files)
                )
                if (
                    num_recent_or_since_dt > len(sorted_sftp_files)
                    or num_recent_or_since_dt < 1
                ):
                    num_recent_or_since_dt = len(sorted_sftp_files)
            elif isinstance(num_recent_or_since_dt, datetime):
                base_end_index = 0
                current_file_dt = datetime(
                    2400, 1, 1
                )  # placeholder to get into the for loop :)
                num_sorted_files = len(sorted_sftp_files)
                while (
                    current_file_dt.date() >= num_recent_or_since_dt.date()
                    and base_end_index < num_sorted_files
                ):
                    current_file_dt = sorted_sftp_files[base_end_index][0]
                    base_end_index += 1
                # if base_end_index ==
                num_recent_or_since_dt = base_end_index

            for file_dt, filename in sorted_sftp_files[0:num_recent_or_since_dt]:
                with rjo_sftp_client.open(filename) as sftp_file:
                    sftp_file.prefetch()
                    file_dataframe = pandas.read_csv(sftp_file, sep=",", parse_dates=date_cols_to_parse)  # type: ignore
                    file_dataframe.columns = (
                        file_dataframe.columns.str.lower()
                        .str.strip()
                        .str.replace(" ", "_")
                    )
                    file_dfs.append(file_dataframe)
                    file_datetimes.append(file_dt)

    if len(file_datetimes) == 0:
        logging.warning(
            "Found no recent enough files with basename %s in RJO SFTP", base_file_name
        )
    else:
        logging.info("Found %s files", len(file_datetimes))

    return file_datetimes, file_dfs
