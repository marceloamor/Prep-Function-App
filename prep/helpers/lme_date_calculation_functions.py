from upedata.static_data import Holiday

from dateutil import relativedelta, easter

from typing import Dict, List, Optional
from datetime import date, datetime
import logging


logger = logging.getLogger("prep.helpers")


def get_good_friday_date(year: int) -> date:
    """Returns the Good Friday date for the given year, permitting
    it is between (inclusively) 1583 and 4099.

    :param year: The year to find Good Friday for
    :type year: int
    :return: The Good Friday date
    :rtype: date
    """
    assert year < 4100 and year > 1582
    return easter.easter(year) - relativedelta.relativedelta(days=2)


def get_lme_prompt_map(
    non_prompts: List[date], _current_date=datetime.today()
) -> Dict[date, date]:
    """Using a list of non-prompt dates and the LME rulebook, generates
    a mapping between dates and corresponding valid prompts that they will
    usually roll to.

    This can then be used to calculate cash, 3M dates etc.

    :param non_prompts: List of dates corresponding to non-prompt dates on the
    LME
    :type non_prompts: List[date]
    :return: Map of Date -> Date where the input is any date from tomorrow through
    the next four months can be input and will map to a date that corresponds to
    a valid LME Settlement Business day
    :rtype: Dict[date, date]
    """
    prompt_map: Dict[date, date] = {}
    now_dt = _current_date
    offset_1d = relativedelta.relativedelta(days=1)

    next_good_friday_date = get_good_friday_date(now_dt.year)

    if now_dt.date() > next_good_friday_date:
        # in this case we've already passed easter friday so we'll use the one for next
        # year in this prompt map
        next_good_friday_date = get_good_friday_date(now_dt.year + 1)

    possible_prompt_to_map = now_dt
    while relativedelta.relativedelta(possible_prompt_to_map, now_dt).months < 4:
        valid_prompt_guess = possible_prompt_to_map

        if valid_prompt_guess.date() not in non_prompts:
            # here will just be checking weekends and making sure they roll in the correct
            # direction
            # Sunday handling logic
            while (
                valid_prompt_guess.weekday() == 6
                or valid_prompt_guess.date() in non_prompts
            ):
                valid_prompt_guess += offset_1d
            # Saturday handling logic
            if valid_prompt_guess.weekday() == 5:
                # good friday should always be in there but doesn't hurt to be a bit careful
                if (valid_prompt_guess - offset_1d).date() not in non_prompts + [
                    next_good_friday_date
                ]:
                    # for dates that fall on a saturday with a valid friday before, this friday
                    # will be selected
                    valid_prompt_guess -= offset_1d
                else:
                    # for dates that fall on a saturday without a valid friday before,
                    # the date will roll forward until a valid date is found
                    while (
                        valid_prompt_guess.weekday() >= 5
                        or valid_prompt_guess.date() in non_prompts
                    ):
                        valid_prompt_guess += offset_1d
            prompt_map[possible_prompt_to_map.date()] = valid_prompt_guess.date()

        else:
            # and now the joys of non-prompts
            if valid_prompt_guess.date() == next_good_friday_date:
                while valid_prompt_guess in non_prompts:
                    valid_prompt_guess -= offset_1d
            elif (
                valid_prompt_guess.month == 12
                and valid_prompt_guess.day == 25
                and valid_prompt_guess.weekday() in (1, 2, 3, 4)
            ):
                while valid_prompt_guess in non_prompts:
                    valid_prompt_guess -= offset_1d
            else:
                while valid_prompt_guess in non_prompts:
                    valid_prompt_guess += offset_1d
            prompt_map[possible_prompt_to_map.date()] = valid_prompt_guess.date()

        possible_prompt_to_map += offset_1d

    return prompt_map


def get_3m_date(current_datetime: datetime, lme_prompt_map: Dict[date, date]) -> date:
    """From the current datetime calculates the LME three month (3M) date,
    requires a valid and up to date `lme_prompt_map` to ensure non-prompts are
    mapped properly.

    :param current_datetime: The datetime to calculate the 3M date for
    :type current_datetime: datetime
    :param lme_prompt_map: LME Prompt map as generated by `get_lme_prompt_map`
    :type lme_prompt_map: Dict[date, date]
    :return: The LME 3M date corresponding to `current_datetime`
    :rtype: date
    """
    guess_3m_datetime = (
        current_datetime + relativedelta.relativedelta(months=3)
    ).date()
    mapped_guess_3m_datetime = lme_prompt_map[guess_3m_datetime]
    i = 1

    while (
        mapped_guess_3m_datetime.month != guess_3m_datetime.month
        and mapped_guess_3m_datetime > guess_3m_datetime
    ):
        mapped_guess_3m_datetime = lme_prompt_map[
            guess_3m_datetime - relativedelta.relativedelta(days=i)
        ]
        if i > 10:
            logger.error(
                "Something has gone very wrong here, ended up stuck in a "
                "loop trying to find a valid 3M date"
            )
            break

    return mapped_guess_3m_datetime


def get_cash_date(
    current_datetime: datetime, lme_product_holidays: List[Holiday]
) -> date:
    full_closure_dates = []
    non_settlement_business_dates = []
    business_days_passed = 0
    current_datetime += relativedelta.relativedelta(hours=4, minutes=29)
    for holiday in lme_product_holidays:
        if holiday.is_closure_date:
            full_closure_dates.append(holiday.holiday_date)
        else:
            non_settlement_business_dates.append(holiday.holiday_date)

    loops = 0
    max_loops = 25
    while loops < max_loops:
        if (
            current_datetime.weekday() > 4
            or current_datetime.date() in full_closure_dates
        ):
            loops += 1
            current_datetime += relativedelta.relativedelta(days=1)
            continue

        if (
            business_days_passed > 1
            and current_datetime.date() not in non_settlement_business_dates
        ):
            # this is the definition of a cash date
            break

        business_days_passed += 1
        loops += 1
        current_datetime += relativedelta.relativedelta(days=1)

    return current_datetime.date()


def get_tom_date(
    current_datetime: datetime, lme_product_holidays: List[Holiday]
) -> Optional[date]:
    full_closure_dates = []
    non_settlement_business_dates = []
    business_days_passed = 0
    current_datetime += relativedelta.relativedelta(hours=4, minutes=29)
    for holiday in lme_product_holidays:
        if holiday.is_closure_date:
            full_closure_dates.append(holiday.holiday_date)
        else:
            non_settlement_business_dates.append(holiday.holiday_date)

    loops = 0
    max_loops = 25
    business_days_passed = 0
    while loops < max_loops:
        if (
            current_datetime.weekday() > 4
            or current_datetime.date() in full_closure_dates
        ):
            pass
        elif current_datetime.date() in non_settlement_business_dates:
            if business_days_passed != 0:
                return None
            business_days_passed += 1
        elif business_days_passed != 0:
            return current_datetime.date()
        else:
            business_days_passed += 1

        loops += 1
        current_datetime += relativedelta.relativedelta(days=1)

    return None
