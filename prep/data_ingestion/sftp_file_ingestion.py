import os
import logging
import tempfile
from datetime import datetime
from typing import List, Tuple

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


def download_file_from_rjo_sftp(formats_to_fetch: List[str]) -> List[str]:
    files_downloaded = []
    
    # use the temporary directory provided by azfunctions
    temp_dir = tempfile.gettempdir()

    with rjo_sftp_utils.get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/OvernightReports")
            for file_format in formats_to_fetch:
                sftp_files: List[Tuple[datetime, str]] = []
                for filename in rjo_sftp_client.listdir():
                    try:
                        file_date = datetime.strptime(filename, file_format)
                        sftp_files.append((file_date, filename))
                    except ValueError:
                        continue

                sftp_files.sort(key=lambda x: x[0], reverse=True)
                
                if len(sftp_files) == 0:
                    logging.warning(
                        f"Found no recent enough files with basename {file_format} in RJO SFTP"
                    )
                    return []
                
                most_recent_file = sftp_files[0][1]
                
                # construct full path for the temp file
                temp_file_path = os.path.join(temp_dir, most_recent_file)
                
                try:
                    rjo_sftp_client.get(
                        most_recent_file,
                        temp_file_path,
                    )
                    # files_downloaded.append(temp_file_path)
                    files_downloaded.append(most_recent_file)
                except FileNotFoundError:
                    logging.error(f"File {most_recent_file} not found in RJO SFTP")
                    return []
                
            logging.info(f"File {most_recent_file} has been successfully downloaded")
            return files_downloaded


def post_file_to_upe_sftp(file_names: List[str]) -> None:
    temp_dir = tempfile.gettempdir()
    
    with rjo_sftp_utils.get_upe_ssh_client() as upe_ssh:
        with upe_ssh.open_sftp() as upe_sftp_client:
            upe_sftp_client.chdir("/rjo_file_backup")
            for file_name in file_names:
                # construct full path for each file in the temp directory
                file_path = os.path.join(temp_dir, file_name)
                upe_sftp_client.put(file_path, file_name)
                logging.info(
                    f"File {file_name} has been successfully posted to UPE SFTP"
                )
            logging.info(f"All files have been successfully posted to UPE SFTP")
            return None
        
# required for historical_migration_script, no longer needed for daily_ingestion_script as using tempfile
def clear_temp_assets_after_upload():
    # get list of files in the temp_assets folder
    files = os.listdir("prep/data_ingestion/temp_assets")
    # delete each file
    for file in files:
        os.remove(f"prep/data_ingestion/temp_assets/{file}")


# not to run for prep daily ingestion, only for historical migration
def historical_migration_script():
    """
    util function to migrate all historical files from RJO SFTP to UPE SFTP following the specified format
    specify the file name in the filename_pattern variable, and optionally edit the date condition
    run this function for each desired file type
    """
    with rjo_sftp_utils.get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/OvernightReports")
            sftp_files: List[Tuple[datetime, str]] = []
            # filename_pattern = f"{file_name}_%Y%m%d.csv"
            filename_pattern = (
                "UPETRADING_statement_dstm_%Y%m%d.pdf"  # SPECIFY FILE NAME HERE
            )
            for filename in rjo_sftp_client.listdir():
                try:
                    file_date = datetime.strptime(filename, filename_pattern)
                    # only add if the date is after 25/07/2024
                    if file_date > datetime(2024, 9, 3):
                        sftp_files.append((file_date, filename))
                    #sftp_files.append((file_date, filename))
                except ValueError:
                    continue

            for file in sftp_files:
                try:
                    rjo_sftp_client.get(
                        file[1], f"prep/data_ingestion/temp_assets/{file[1]}"
                    )
                    print(f"File {file[1]} has been successfully downloaded")
                except FileNotFoundError:
                    logging.error(f"File {file[1]} not found in RJO SFTP")
                    continue

            # post
            with rjo_sftp_utils.get_upe_ssh_client() as upe_ssh:
                with upe_ssh.open_sftp() as upe_sftp_client:
                    upe_sftp_client.chdir("/rjo_file_backup")
                    for file in sftp_files:
                        upe_sftp_client.put(
                            f"prep/data_ingestion/temp_assets/{file[1]}", file[1]
                        )
                        logging.info(
                            f"File {file[1]} has been successfully posted to UPE SFTP"
                        )
                        print(
                            f"File {file[1]} has been successfully posted to UPE SFTP"
                        )

            # clear temp assets
            clear_temp_assets_after_upload()
            print("Migration complete")

            return "Migration complete"

# historical_migration_script()

# previous iterations, before using tempfiles
def download_file_from_rjo_sftp_local(formats_to_fetch: List[str]) -> List[str]:
    files_downloaded = []
    with rjo_sftp_utils.get_rjo_ssh_client() as rjo_ssh:
        with rjo_ssh.open_sftp() as rjo_sftp_client:
            rjo_sftp_client.chdir("/OvernightReports")
            for file_format in formats_to_fetch:
                sftp_files: List[Tuple[datetime, str]] = []
                for filename in rjo_sftp_client.listdir():
                    try:
                        file_date = datetime.strptime(filename, file_format)
                        sftp_files.append((file_date, filename))
                    except ValueError:
                        continue

                sftp_files.sort(key=lambda x: x[0], reverse=True)
                most_recent_file = sftp_files[0][1]
                if len(sftp_files) == 0:
                    logging.warning(
                        f"Found no recent enough files with basename {file_format} in RJO SFTP"
                    )
                    return []
                # download the most recent file into the prep/data_ingestion/temp_assets folder
                try:
                    rjo_sftp_client.get(
                        sftp_files[0][1],
                        f"prep/data_ingestion/temp_assets/{most_recent_file}",
                    )
                    files_downloaded.append(most_recent_file)
                except FileNotFoundError:
                    logging.error(f"File {most_recent_file} not found in RJO SFTP")
                    return []
                
            logging.info(f"File {most_recent_file} has been successfully downloaded")
            return files_downloaded
        

def post_file_to_upe_sftp_local(file_names: List[str]) -> None:
    base_path = "prep/data_ingestion/temp_assets/"
    with rjo_sftp_utils.get_upe_ssh_client() as upe_ssh:
        with upe_ssh.open_sftp() as upe_sftp_client:
            upe_sftp_client.chdir("/rjo_file_backup")
            for file_name in file_names:
                file_path = base_path + file_name
                upe_sftp_client.put(file_path, file_name)
                logging.info(
                    f"File {file_name} has been successfully posted to UPE SFTP"
                )
            logging.info(f"File {file_name} has been successfully posted to UPE SFTP")
            return None