import copy
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Dict, List, Optional

from dateutil import easter, relativedelta
from upedata.static_data import Holiday
from zoneinfo import ZoneInfo

_DEFAULT_FORWARD_MONTHS = 18


@dataclass
class LMEFuturesCurve:
    cash: datetime
    three_month: datetime
    weeklies: List[datetime]
    monthlies: List[datetime]
    prompt_map: Dict[date, date]
    tom: Optional[datetime] = None
    broken_dates: List[datetime] = field(default_factory=list)

    # possibly the most disgustingly *pythonic* thing I've ever written...
    # sorry to anyone that has to look at this
    def populate_broken_datetimes(self):
        """Populates the broken dates within the curve in-place, will
        not include TOM, CASH, or 3M, but may overlap with monthlies
        or weeklies.
        """

        def within_broken_date_window(dt_to_check):
            return cash_date < dt_to_check < three_month_date

        logging.debug("Populating `LMEFuturesCurve` broken datetimes")
        cash_date = self.cash.date()
        three_month_date = self.three_month.date()
        europe_london_tz = ZoneInfo("Europe/London")
        expiry_time = time(19, 00, tzinfo=europe_london_tz)

        self.broken_dates = sorted(
            {
                datetime.combine(filtered_date, expiry_time)
                for filtered_date in set(self.prompt_map.values())
                if within_broken_date_window(filtered_date)
            }
        )

    def gen_prompt_list(self) -> List[datetime]:
        """Returns a sorted list of all prompts contained in this
        dataclass

        :return: Sorted list of prompt datetimes
        :rtype: List[datetime]
        """
        logging.debug("Generating prompt list from `LMEFuturesCurve`")
        prompt_set = set(
            [self.cash, self.three_month]
            + self.weeklies
            + self.monthlies
            + self.broken_dates
        )
        if self.tom is not None:
            prompt_set.add(self.tom)
        return sorted(list(prompt_set))


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
    non_prompts: List[date], _current_datetime=None
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
    if not isinstance(_current_datetime, datetime):
        now_dt = datetime.now(tz=ZoneInfo("Europe/London"))
    else:
        now_dt = _current_datetime
    offset_1d = relativedelta.relativedelta(days=1)

    next_good_friday_date = get_good_friday_date(now_dt.year)

    if now_dt.date() > next_good_friday_date:
        # in this case we've already passed easter friday so we'll use the one for next
        # year in this prompt map
        next_good_friday_date = get_good_friday_date(now_dt.year + 1)

    possible_prompt_to_map = now_dt
    while relativedelta.relativedelta(possible_prompt_to_map, now_dt).months < 4:
        valid_prompt_guess = copy.copy(possible_prompt_to_map)

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
        else:
            # and now the joys of non-prompts
            if valid_prompt_guess.date() == next_good_friday_date:
                while valid_prompt_guess.date() in non_prompts + [
                    next_good_friday_date
                ]:
                    valid_prompt_guess -= offset_1d
            elif (
                valid_prompt_guess.month == 12
                and valid_prompt_guess.day == 25
                and valid_prompt_guess.weekday() in (1, 2, 3, 4)
            ):
                while valid_prompt_guess.date() in non_prompts:
                    valid_prompt_guess -= offset_1d
            else:
                while (
                    valid_prompt_guess.date() in non_prompts
                    or valid_prompt_guess.weekday() >= 5
                ):
                    valid_prompt_guess += offset_1d

        prompt_map[possible_prompt_to_map.date()] = valid_prompt_guess.date()
        possible_prompt_to_map += offset_1d

    return prompt_map


def get_3m_datetime(
    current_datetime: datetime, lme_prompt_map: Dict[date, date]
) -> datetime:
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
        current_datetime + relativedelta.relativedelta(months=3, hours=4, minutes=29)
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
            logging.error(
                "Something has gone very wrong here, ended up stuck in a "
                "loop trying to find a valid 3M date\n%s\n%s\n%s",
                current_datetime,
                guess_3m_datetime,
                mapped_guess_3m_datetime,
            )
            break

    return datetime.combine(
        mapped_guess_3m_datetime, time(19, 0, tzinfo=ZoneInfo("Europe/London"))
    )


def get_cash_datetime(
    current_datetime: datetime, lme_product_holidays: List[Holiday]
) -> datetime:
    """Calculates the cash date from the current datetime and a set of LME
    "holidays".

    Cash (LME Rulebook, Part 1.1 Definitions, Page 1-4 January 2023 Edition)

    in relation to the period between 19.31 hours on one Business Day and
    19.30 hours on the next Business Day and Contracts entered into in
    that period, the first Settlement Business Day which falls after the
    next following Business Day (also referred to as "SPOT");

    :param current_datetime: The current datetime
    :type current_datetime: datetime
    :param lme_product_holidays: A list of LME holidays, closure days,
    non-settlement days, etc.
    :type lme_product_holidays: List[Holiday]
    :return: The calculated cash date
    :rtype: date
    """
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

    return current_datetime.replace(
        hour=19, minute=0, second=0, microsecond=0, tzinfo=ZoneInfo("Europe/London")
    )


def get_tom_datetime(
    current_datetime: datetime, lme_product_holidays: List[Holiday]
) -> Optional[datetime]:
    """Calculated the "Cash Today" or "TOM" date from the current datetime
    if there is one, else returns `None`.

    Cash Today (LME Rulebook, Part 1.1 Definitions, Page 1-5 January 2023 Edition)

    in relation to Contracts entered into in the period between 19.31 hours on one
    Business Day and 12.30 hours on the next Business Day, the first Settlement
    Business Day after the latter Business Day save that there will be no Prompt
    Date for Cash Today where Cash Today is a Business Day but not a Settlement
    Business Day (also referred to as "TOM" or "tomorrow");

    :param current_datetime: The current datetime
    :type current_datetime: datetime
    :param lme_product_holidays: A list of LME holidays, closure days,
    non-settlement days, etc.
    :type lme_product_holidays: List[Holiday]
    :return: The calculated TOM date, if there is one, else None
    :rtype: Optional[date]
    """
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
            return current_datetime.replace(
                hour=19,
                minute=00,
                second=0,
                microsecond=0,
                tzinfo=ZoneInfo("Europe/London"),
            )
        else:
            business_days_passed += 1

        loops += 1
        current_datetime += relativedelta.relativedelta(days=1)

    return None


def get_valid_monthly_prompts(
    current_datetime: datetime, forward_months=18
) -> List[datetime]:
    """Generates a list of the monthly prompt dates for the LME

    :param current_datetime: The current datetime
    :type current_datetime: datetime
    :param forward_months: Number of months forward to generate, defaults to 18
    :type forward_months: int, optional
    :return: List of LME monthly forward prompt dates
    :rtype: List[datetime]
    """
    one_month_offset = relativedelta.relativedelta(months=1)
    if current_datetime.tzinfo is None:
        current_datetime.replace(tzinfo=ZoneInfo("Europe/London"))

    third_wednesday_monthly_prompts = []
    loops = 0
    while loops < forward_months:
        third_wednesday_monthly_prompts.append(
            current_datetime
            + relativedelta.relativedelta(
                day=1,
                weekday=relativedelta.WE(3),
                hour=19,
                minute=0,
                second=0,
                microsecond=0,
            )
        )
        current_datetime += one_month_offset
        loops += 1

    return third_wednesday_monthly_prompts


def get_all_valid_weekly_prompts(
    current_datetime: datetime, lme_prompt_map: Dict[date, date]
) -> List[datetime]:
    """Generates a list of all valid weekly LME prompts, all of which
    are after the 3M date.

    (LME Rulebook, Part 8.1 Prompt Dates, Page 3-33 January 2023 Edition)
        Metal Futures may have any of the following Prompt Dates (but subject, where relevant, to
    sub-paragraph 8.2 and 8.4 of this Regulation):-
        ...

        (d)  each Wednesday falling after the three-months date until and including the last
        Wednesday in the sixth calendar month after the calendar month in which the Contract
        is made;

        ...

    :param current_datetime: The current datetime
    :type current_datetime: datetime
    :param lme_prompt_map: LME prompt map as generated by
    `get_lme_prompt_map`
    :type lme_prompt_map: Dict[date, date]
    :return: List of valid LME weekly prompts
    :rtype: List[datetime]
    """
    current_3m_date: date = get_3m_datetime(current_datetime, lme_prompt_map)
    next_wednesday = current_3m_date + relativedelta.relativedelta(
        days=1, weekday=relativedelta.WE(1)
    )
    weekly_prompt_dates = []
    while (
        relativedelta.relativedelta(
            next_wednesday + relativedelta.relativedelta(day=1), current_datetime
        ).months
        < 6
    ):
        weekly_prompt_dates.append(next_wednesday)
        next_wednesday += relativedelta.relativedelta(
            weeks=1, hour=19, minute=0, second=0, microsecond=0
        )

    return weekly_prompt_dates


def populate_primary_curve_datetimes(
    non_prompts: List[date],
    product_holidays: List[Holiday],
    forward_months=_DEFAULT_FORWARD_MONTHS,
    _current_datetime=None,
) -> LMEFuturesCurve:
    """Generates and populates a container dataclass with the primary
    prompt dates associated with a given LME product, this will
    provide: TOM, CASH, 3M, weeklies, and monthlies with no guarantee
    of uniqueness of prompt between these fields.

    :param non_prompts: List of all LME non-prompt dates
    :type non_prompts: List[date]
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param forward_months: Number of months of monthly futures to generate, also corresponds
    to the number of options generated as these are derivative of the monthly futures,
    defaults to 18
    :type forward_months: int, optional
    :return: Container for LME product future prompt datetimes
    :rtype: LMEFuturesCurve
    """
    if not isinstance(_current_datetime, datetime):
        _current_datetime = datetime.now(tz=ZoneInfo("Europe/London"))
    lme_prompt_map = get_lme_prompt_map(
        non_prompts, _current_datetime=_current_datetime
    )
    lme_3m_datetime = get_3m_datetime(_current_datetime, lme_prompt_map)
    lme_cash_datetime = get_cash_datetime(_current_datetime, product_holidays)
    lme_tom_datetime = get_tom_datetime(_current_datetime, product_holidays)
    lme_weekly_datetimes = get_all_valid_weekly_prompts(
        _current_datetime, lme_prompt_map
    )
    lme_monthly_datetimes = get_valid_monthly_prompts(
        _current_datetime, forward_months=forward_months
    )
    return LMEFuturesCurve(
        lme_cash_datetime,
        lme_3m_datetime,
        lme_weekly_datetimes,
        lme_monthly_datetimes,
        lme_prompt_map,
        tom=lme_tom_datetime,
    )
