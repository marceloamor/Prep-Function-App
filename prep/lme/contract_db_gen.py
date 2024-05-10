import contract_param_gen
import date_calc_funcs
import upedata.static_data as upestatic
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import insert as pg_insert

LME_PRODUCT_NAMES = ["AHD", "CAD", "PBD", "ZSD", "NID"]
LME_METAL_NAMES = ["aluminium", "copper", "lead", "zinc", "nickel"]
LME_FUTURE_MULTIPLIERS_LIST = [25, 25, 25, 25, 6]
GEORGIA_LME_PRODUCT_NAMES_BASE = [
    "lad",
    "lcu",
    "pbd",
    "lzh",
    "lnd",
]
CQG_3M_FEEDS = ["X.US.LALZ", "X.US.LDKZ", "X.US.LEDZ", "X.US.LZHZ", "X.US.LNIZ"]
LME_FUTURE_MULTIPLIERS = {
    georgia_product_name: product_multiplier
    for georgia_product_name, product_multiplier in zip(
        GEORGIA_LME_PRODUCT_NAMES_BASE, LME_FUTURE_MULTIPLIERS_LIST
    )
}
LME_FUTURE_3M_FEED_ASSOC = {
    georgia_product_name: future_3m_feed
    for georgia_product_name, future_3m_feed in zip(
        GEORGIA_LME_PRODUCT_NAMES_BASE, CQG_3M_FEEDS
    )
}
LME_PRODUCT_NAME_MAP = {
    lme_product_name[0:2]: lme_metal_name
    for lme_product_name, lme_metal_name in zip(
        GEORGIA_LME_PRODUCT_NAMES_BASE, LME_METAL_NAMES
    )
}


def add_futures_to_database(
    lme_futures_expiry_dates: date_calc_funcs.LMEFuturesCurve,
    product_symbol: str,
    db_session: orm.Session,
):
    short_code = product_symbol.lstrip("xlme-").rstrip("-usd").lower()
    future_params = [
        contract_param_gen.generate_future_params(
            product_symbol,
            expiry_dt,
            LME_FUTURE_MULTIPLIERS[short_code],
            f"{short_code} {expiry_dt.strftime(r'%Y-%m-%d')}",
            {
                "form": "physical",
                "time": ["expiry", "0"],
                "style": "forward",
                "version": "1.1",
            },
        )
        for expiry_dt in lme_futures_expiry_dates.gen_prompt_list()
    ]
    insert_futures = (
        pg_insert(upestatic.Future)
        .values(future_params)
        .on_conflict_do_nothing()
        .returning(upestatic.Future.symbol)
    )
