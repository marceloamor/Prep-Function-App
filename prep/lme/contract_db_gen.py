import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import sqlalchemy
import upedata.dynamic_data as upedynamic
import upedata.enums as upeenums
import upedata.static_data as upestatic
import upedata.template_language.parser as upeparse
from dateutil.relativedelta import WE, relativedelta
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import insert as pg_insert

from prep.exceptions import ProductNotFound
from prep.lme import contract_param_gen, date_calc_funcs

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
    short_code = product_symbol.split("-")[1]
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
    logging.debug(inserted_future_symbols)
    if len(inserted_future_symbols) == 0:
        return []
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

    full_expiry_set = set(expiry_dts)
    new_expiry_dts = (
        sqlalchemy.select(upestatic.Option.expiry)
        .where(upestatic.Option.product_symbol == product.symbol)
        .where(upestatic.Option.expiry.in_(expiry_dts))
    )
    new_expiry_dts = set(db_session.execute(new_expiry_dts).scalars().all())
    new_expiry_dts = full_expiry_set.difference(new_expiry_dts)

    logging.debug(new_expiry_dts)
    logging.debug(option_data)
    vol_surface_params = [
        contract_param_gen.generate_vol_surface(model_type, expiry_dt, model_params)
        for expiry_dt in new_expiry_dts
    ]
    if len(vol_surface_params) == 0:
        return []

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
        logging.debug(option_param)
        placeholder_future.expiry = option_param["expiry"] + relativedelta(
            day=1, weekday=WE(3), hour=19, minute=0
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


def update_lme_static_data(pg_session: orm.Session, months_ahead=20):
    with open("./prep/helpers/data_files/lme_option_base_data.json") as fp:
        option_spec_data = json.load(fp)
    for product_symbol, prod_specific_op_data in option_spec_data["specific"].items():
        prod_specific_op_data |= option_spec_data["shared"]
        product = pg_session.get(upestatic.Product, product_symbol)
        if product is None:
            raise ProductNotFound(
                f"Unable to find `{product_symbol}` in products table"
            )
        holiday_dates = [holiday.holiday_date for holiday in product.holidays]
        lme_futures_curve = date_calc_funcs.populate_primary_curve_datetimes(
            holiday_dates, product.holidays, forward_months=months_ahead
        )
        lme_futures_curve.populate_broken_datetimes()
        futures_prompt_list = lme_futures_curve.gen_prompt_list()

        option_expiry_dts = [
            future_expiry + relativedelta(day=1, weekday=WE(1), hour=11, minute=15)
            for future_expiry in lme_futures_curve.monthlies
        ]

        new_futures = add_futures_to_database(
            futures_prompt_list, product.symbol, pg_session
        )
        logging.info("Added %s futures for %s", len(new_futures), product.symbol)

        new_options = add_options_to_database(
            option_expiry_dts, product, prod_specific_op_data, pg_session
        )
        logging.info("Added %s options for %s", len(new_options), product.symbol)
