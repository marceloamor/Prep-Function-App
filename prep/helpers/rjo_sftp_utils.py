import paramiko.client

from typing import Tuple, List
from datetime import datetime


def get_lme_overnight_data(
    base_file_name: str, fetch_most_recent_num=1
) -> Tuple[List[datetime], List[None]]:
    pass
