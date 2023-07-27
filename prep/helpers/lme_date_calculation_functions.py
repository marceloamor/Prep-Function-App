from dateutil import relativedelta, easter

from datetime import date, datetime
from typing import Dict, List


def get_lme_non_prompt_map(non_prompts: List[date]) -> Dict[date, date]:
    non_prompt_map = {}
    offset_1d = relativedelta.relativedelta(days=1)
    easter_friday_date = easter.easter(
        datetime.now().year()
    ) - relativedelta.relativedelta(days=2)
    for non_prompt in non_prompts:
        day = non_prompt.day
        month = non_prompt.month
        weekday = non_prompt.weekday
        is_rollback_friday_nonprompt = False
        if non_prompt == easter_friday_date:
            # easter friday prompts fall on the prior day
            non_prompt_map[non_prompt] = non_prompt - offset_1d
            is_rollback_friday_nonprompt = True
        elif month == 12 and day == 25 and weekday in (1, 2, 3, 4):
            # christmas day if it's a tuesday through friday is the prior settlement day
            non_prompt_map[non_prompt] = non_prompt - offset_1d
            is_rollback_friday_nonprompt = True
        else:
            # all other non-prompts roll forwards to the next succeeding day which is a
            # settlement business day rule (8.4.1)
            next_valid_settlement_day_guess = non_prompt + offset_1d
            # while next_valid_settlement_day_guess in
            non_prompt_map[non_prompt] = non_prompt

        if is_rollback_friday_nonprompt:
            # specific fridays will roll back but the subsequent saturday needs to be
            # mapped to the following monday
            non_prompt_map[
                non_prompt + offset_1d
            ] = non_prompt + relativedelta.relativedelta(days=3)
        # finish writing cases make sure that they don't overlap or end up in endless loops
        # probably requires a verification step


def get_3m_date(current_datetime: datetime, holiday_date_map: Dict[date, date]) -> date:
    guess_3m_datetime = current_datetime + relativedelta.relativedelta(months=3)
    offset_1d = relativedelta.relativedelta(days=1)
