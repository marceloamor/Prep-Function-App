from upedata.static_data import (
    Product,
    Future,
    PriceFeed,
    FuturePriceFeedAssociation,
    Option,
)

from upedata.template_language import parser
from upedata.dynamic_data import VolSurface
from prep.exceptions import ProductNotFound
import upedata.enums as upe_enums

from dateutil.relativedelta import relativedelta
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict


LME_FUTURE_MULTIPLIERS = {"lad": 25, "lcu": 25, "pbd": 25, "lzh": 25, "lnd": 6}
LME_FUTURE_3M_FEED_ASSOC = {
    "lad": "X.US.LALZ",
    "lcu": "X.US.LDKZ",
    "pbd": "X.US.LEDZ",
    "lzh": "X.US.LZHZ",
    "lnd": "X.US.LNIZ",
}


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
