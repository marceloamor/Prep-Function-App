from datetime import datetime
from typing import Any, Dict, List

import contract_param_gen
import sqlalchemy
import upedata.dynamic_data as upedynamic
import upedata.enums as upeenums
import upedata.static_data as upestatic
import upedata.template_language.parser as upeparse
from dateutil.relativedelta import relativedelta
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
    expiry_dts: List[datetime],
    product_symbol: str,
    db_session: orm.Session,
) -> List[str]:
    short_code = product_symbol.lstrip("xlme-").rstrip("-usd").lower()
    multiplier = LME_FUTURE_MULTIPLIERS[short_code]
    base_feed_id = LME_FUTURE_3M_FEED_ASSOC[short_code]
    future_params = []
    price_feed_assocs = []
    price_feeds = contract_param_gen.generate_future_price_feeds_params(
        base_feed_id, "cqg"
    )
    for expiry_dt in expiry_dts:
        future_params.append(
            contract_param_gen.generate_future_params(
                product_symbol,
                expiry_dt,
                multiplier,
                {
                    "form": "physical",
                    "time": ["expiry", "0"],
                    "style": "forward",
                    "version": "1.1",
                },
                f"{short_code} {expiry_dt.strftime(r'%Y-%m-%d')}".upper(),
            )
        )

    insert_futures_r_fut_sym = (
        pg_insert(upestatic.Future)
        .values(future_params)
        .on_conflict_do_nothing()
        .returning(upestatic.Future.symbol)
    )
    insert_price_feeds = (
        pg_insert(upestatic.PriceFeed).values(price_feeds).on_conflict_do_nothing()
    )

    inserted_future_symbols = (
        db_session.execute(insert_futures_r_fut_sym).scalars().all()
    )
    db_session.execute(insert_price_feeds)

    for new_future_symbol in inserted_future_symbols:
        price_feed_assocs.extend(
            contract_param_gen.generate_future_price_feed_associations_params(
                new_future_symbol, base_feed_id, "cqg"
            )
        )

    add_price_feed_assocs = (
        pg_insert(upestatic.FuturePriceFeedAssociation)
        .values(price_feed_assocs)
        .on_conflict_do_nothing()
    )
    db_session.execute(add_price_feed_assocs)

    return list(inserted_future_symbols)


def add_options_to_database(
    expiry_dts: List[datetime],
    product: upestatic.Product,
    option_data: Dict[str, Any],
    db_session: orm.Session,
) -> List[str]:
    placeholder_future = upestatic.Future()
    vol_surface_params = []
    option_params = []
    multiplier = option_data["multiplier"]
    strike_intervals = option_data["strike_intervals"]
    time_type = upeenums.TimeType(option_data["time_type"])
    vol_type = upeenums.VolType(option_data["vol_type"])

    model_type = option_data["vol_surface"]["model_type"]
    model_params = option_data["vol_surface"]["params"]

    transient_table_subq = sqlalchemy.values(
        sqlalchemy.column("poss_exp_dt", sqlalchemy.DateTime),
        name="poss_expiry_cte",
    ).data([(expiry_dt,) for expiry_dt in expiry_dts])
    new_expiry_dts = (
        sqlalchemy.select(transient_table_subq.c.poss_exp_dt)
        .join(
            upestatic.Option.expiry,
            upestatic.Option.expiry == transient_table_subq.c.poss_exp_dt,
        )
        .where(upestatic.Option.expiry.is_(None))
    )
    new_expiry_dts = db_session.execute(new_expiry_dts).scalars().all()

    vol_surface_params = [
        contract_param_gen.generate_vol_surface(model_type, expiry_dt, model_params)
        for expiry_dt in new_expiry_dts
    ]
    insert_vol_surfaces = (
        pg_insert(upedynamic.VolSurface)
        .values(vol_surface_params)
        .on_conflict_do_nothing()
        .returning(upedynamic.VolSurface.vol_surface_id, upedynamic.VolSurface.expiry)
    )
    inserted_vol_surface_data = db_session.execute(insert_vol_surfaces).tuples().all()

    option_params = []
    for vol_surface_id, vol_surface_expiry in inserted_vol_surface_data:
        # this isn't great, if someone can bring themselves to rewrite
        # the parser chunk of upedata that'd allow the simplification
        # of this code and removal of transient objects that exist to
        # generate a single string :/
        # I blame orm for not allowing on-conflict statements at time
        # of writing... very rude
        und_future_expiry = vol_surface_expiry + relativedelta(days=14)
        und_future_symbol = (
            f"{product.symbol} f {und_future_expiry.strftime(r'%y-%m-%d')}"
        )
        option_param = contract_param_gen.generate_option_params(
            product.symbol,
            vol_surface_id,
            und_future_symbol,
            strike_intervals,
            time_type,
            multiplier,
            vol_type,
            vol_surface_expiry,
            None,
        )
        option_obj = upestatic.Option(
            symbol=option_param["symbol"],
            multiplier=option_param["multiplier"],
            strike_intervals=option_param["strike_intervals"],
            expiry=option_param["expiry"],
            display_name=option_data["display_name"],
            product_symbol=option_param["product_symbol"],
            underlying_future_symbol=option_param["underlying_future_symbol"],
            vol_surface_id=option_param["vol_surface_id"],
            vol_type=option_param["vol_type"],
            time_type=option_param["time_type"],
            product=product,
            underlying_future=placeholder_future,
        )
        option_obj = upeparse.substitute_derivative_generation_time(option_obj)
        option_param["display_name"] = option_obj.display_name
        option_params.append(option_param)

    insert_options = (
        pg_insert(upestatic.Option).values(option_params).on_conflict_do_nothing()
    ).returning(upestatic.Option.symbol)

    new_options = db_session.execute(insert_options).scalars().all()

    return list(new_options)
