from prep.helpers import lme_date_calc_funcs

from upedata.static_data import (
    FuturePriceFeedAssociation,
    PriceFeed,
    Product,
    Holiday,
    Future,
    Option,
)

from upedata.template_language import parser
from upedata.dynamic_data import VolSurface
from prep.exceptions import ProductNotFound
import upedata.enums as upe_enums

from dateutil.relativedelta import relativedelta
from typing import List, Dict, Tuple, Optional
from datetime import datetime, date, time
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo
import json


LME_FUTURE_MULTIPLIERS = {"lad": 25, "lcu": 25, "pbd": 25, "lzh": 25, "lnd": 6}
LME_FUTURE_3M_FEED_ASSOC = {
    "lad": "X.US.LALZ",
    "lcu": "X.US.LDKZ",
    "pbd": "X.US.LEDZ",
    "lzh": "X.US.LZHZ",
    "lnd": "X.US.LNIZ",
}


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
        cash_date = self.cash.date()
        three_month_date = self.three_month.date()
        europe_london_tz = ZoneInfo("Europe/London")
        expiry_time = time(12, 30, tzinfo=europe_london_tz)
        within_broken_date_window = (
            lambda dt_to_check: cash_date < dt_to_check < three_month_date
        )
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
        prompt_set = set(
            [self.cash, self.three_month]
            + self.weeklies
            + self.monthlies
            + self.broken_dates
        )
        if self.tom is not None:
            prompt_set.add(self.tom)
        return sorted(list(prompt_set))


def fetch_lme_option_specification_data(
    path="./prep/helpers/data_files/lme_option_base_data.json",
) -> Dict:
    with open(path, "r") as fp:
        option_spec_data = json.load(fp)
    return option_spec_data


def gen_lme_futures(expiry_dates: List[datetime], product: Product) -> List[Future]:
    static_data_futures: List[Future] = []
    product_3m_feed = PriceFeed(
        feed_id=LME_FUTURE_3M_FEED_ASSOC[product.short_name],
        origin="cqg",
        delayed=False,
        subscribe=True,
    )
    product_3m_relative_spread_feed = PriceFeed(
        feed_id="SPREAD_RELATIVE_TO_3M",
        origin="local",
        delayed=False,
        subscribe=False,
    )
    for expiry_date in expiry_dates:
        try:
            new_lme_future = Future(
                symbol=f"{product.symbol} f {expiry_date.strftime(r'%y-%m-%d')}",
                display_name=(
                    f"{product.short_name} {expiry_date.strftime(r'%Y-%m-%d')}"
                ).upper(),
                expiry=expiry_date,
                multiplier=LME_FUTURE_MULTIPLIERS[product.short_name],
                product=product,
            )
            product_3m_future_price_feed_assoc = FuturePriceFeedAssociation(
                future=new_lme_future,
                feed=product_3m_feed,
                weighting=1.0,
            )
            product_relative_spread_feed = FuturePriceFeedAssociation(
                future=new_lme_future,
                feed=product_3m_relative_spread_feed,
                weighting=1.0,
            )
            new_lme_future.underlying_feeds = [
                product_3m_future_price_feed_assoc,
                product_relative_spread_feed,
            ]

        except KeyError:
            raise ProductNotFound(
                f"Unable to find {product.short_name} in `LME_FUTURE_MULTIPLIERS`"
            )
        static_data_futures.append(new_lme_future)

    return static_data_futures


def gen_lme_options(
    futures_list: List[Future], option_specification_data: Dict
) -> List[Option]:
    two_week_td = relativedelta(days=14)
    generated_options: List[Option] = []
    for future in futures_list:
        # first need to check to make sure it's on a third Wednesday, as these will
        # always be underlying of an LME option.
        # if not just ignore
        if future.expiry.weekday() == 2:
            if 15 <= future.expiry.day and future.expiry.day <= 21:
                # the timezone fuckery is in case of something *somehow* crossing a dst
                # boundary, odds are this won't happen but you never know.
                option_expiry_date = (
                    future.expiry.replace(tzinfo=ZoneInfo("Europe/London"))
                    - two_week_td
                ).replace(hour=11, minute=15)
                general_option_data = (
                    option_specification_data["specific"][future.product.symbol.lower()]
                    | option_specification_data["shared"]
                )
                generated_option = Option(
                    symbol=f"{future.product.symbol} o {option_expiry_date.strftime(r'%y-%m-%d')} a",
                    multiplier=general_option_data["multiplier"],
                    strike_intervals=general_option_data["strike_intervals"],
                    expiry=option_expiry_date,
                    display_name=option_specification_data["shared"]["display_name"],
                    product=future.product,
                    underlying_future=future,
                    vol_surface=VolSurface(
                        model_type=general_option_data["vol_surface"]["model_type"],
                        expiry=option_expiry_date,
                        params=general_option_data["vol_surface"]["params"],
                    ),
                    vol_type=upe_enums.VolType(general_option_data["vol_type"]),
                    time_type=upe_enums.TimeType(general_option_data["time_type"]),
                )
                generated_option = parser.substitute_derivative_generation_time(
                    generated_option
                )
                generated_options.append(generated_option)

    return generated_options


def populate_primary_curve_datetimes(
    product_symbol: str,
    non_prompts: List[date],
    product_holidays: List[Holiday],
    forward_months=18,
    _current_datetime=datetime.now(tz=ZoneInfo("Europe/London")),
) -> LMEFuturesCurve:
    """Generates and populates a container dataclass with the primary
    prompt dates associated with a given LME product, this will
    provide: TOM, CASH, 3M, weeklies, and monthlies with no guarantee
    of uniqueness of prompt between these fields.

        :param product_symbol: The symbol of the LME product to generate derivatives for
    :type product_symbol: str
    :param non_prompts: List of all LME non-prompt dates
    :type non_prompts: List[date]
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param populate_options: Whether to generate options associated with generated monthly
    futures, defaults to True
    :type populate_options: bool, optional
    :param forward_months: Number of months of monthly futures to generate, also corresponds
    to the number of options generated as these are derivative of the monthly futures,
    defaults to 18
    :type forward_months: int, optional
    :return: Container for LME product future prompt datetimes
    :rtype: LMEFuturesCurve
    """
    lme_prompt_map = lme_date_calc_funcs.get_lme_prompt_map(non_prompts)
    lme_3m_datetime = lme_date_calc_funcs.get_3m_datetime(
        _current_datetime, lme_prompt_map
    )
    lme_cash_datetime = lme_date_calc_funcs.get_cash_datetime(
        _current_datetime, product_holidays
    )
    lme_tom_datetime = lme_date_calc_funcs.get_tom_datetime(
        _current_datetime, product_holidays
    )
    lme_weekly_datetimes = lme_date_calc_funcs.get_all_valid_weekly_prompts(
        _current_datetime, lme_prompt_map
    )
    lme_monthly_datetimes = lme_date_calc_funcs.get_valid_monthly_prompts(
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


def populate_full_curve(
    product: Product,
    non_prompts: List[date],
    product_holidays: List[Holiday],
    populate_options=True,
    forward_months=18,
    _current_datetime=datetime.now(tz=ZoneInfo("Europe/London")),
) -> Tuple[LMEFuturesCurve, List[Future], List[Option]]:
    """Generates the futures and options (if `populate_options` is `True`) across the
    entire curve within the limit given by `months_forward`.
    Will provide all valid 3M and weekly prompt futures (runs to 6 months) and all monthly
    prompts to the limit of `months_forward`.

    :param product_symbol: The static data represtentation of the LME product for which
    to generate derivatives
    :type product_symbol: Product
    :param non_prompts: List of all LME non-prompt dates
    :type non_prompts: List[date]
    :param product_holidays: List of all product holidays
    :type product_holidays: List[Holiday]
    :param populate_options: Whether to generate options associated with generated monthly
    futures, defaults to True
    :type populate_options: bool, optional
    :param forward_months: Number of months of monthly futures to generate, also corresponds
    to the number of options generated as these are derivative of the monthly futures,
    defaults to 18
    :type forward_months: int, optional
    :return: A tuple containing the LMEFuturesCurve, a list of all futures generated and
    the list of all options generated, the latter will be an empty list if
    `populate_options is False`
    :rtype: Tuple[LMEFuturesCurve, List[Future], List[Option]]
    """

    lme_futures_curve = populate_primary_curve_datetimes(
        product.symbol,
        non_prompts,
        product_holidays,
        forward_months=forward_months,
        _current_datetime=_current_datetime,
    )
    lme_futures_curve.populate_broken_datetimes()

    future_expiries = lme_futures_curve.gen_prompt_list()
    futures = gen_lme_futures(future_expiries, product)
    if populate_options:
        options = gen_lme_options(futures, fetch_lme_option_specification_data())
    else:
        options = []

    return lme_futures_curve, futures, options
