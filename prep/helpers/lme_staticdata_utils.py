from upedata.static_data import Product, Future, PriceFeed, FuturePriceFeedAssociation
from prep.helpers.date_name_utilities import MONTH_CODE_MAPPING
from prep.exceptions import ProductNotFound

from datetime import datetime, date
from typing import List


LME_FUTURE_MULTIPLIERS = {"lad": 25, "lcu": 25, "pbd": 25, "lzh": 25, "lnd": 6}
LME_FUTURE_3M_FEED_ASSOC = {
    "lad": "X.US.LALZ",
    "lcu": "X.US.LDKZ",
    "pbd": "X.US.LEDZ",
    "lzh": "X.US.LZHZ",
    "lnd": "X.US.LNIZ",
}


def gen_lme_futures(
    expiry_dates: List[datetime], product: Product, generate_options=False
) -> List[Future]:
    static_data_futures = []
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
            if generate_options:
                pass

        except KeyError:
            raise ProductNotFound(
                f"Unable to find {product.short_name} in `LME_FUTURE_MULTIPLIERS`"
            )
        static_data_futures.append(new_lme_future)

    return static_data_futures
