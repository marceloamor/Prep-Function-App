from datetime import datetime
from typing import Any, Dict, List, Optional

from upedata import enums as upeenums


def generate_future_params(
    product_symbol: str,
    expiry_datetime: datetime,
    multiplier: int,
    settlement: Dict[str, Any],
    display_name: Optional[str] = None,
) -> Dict[str, Any]:
    if expiry_datetime.tzinfo is None:
        raise ValueError(
            "Expiry for future was given without timezone: "
            f"{product_symbol} {expiry_datetime}"
        )
    future_params = {
        "symbol": f"{product_symbol} f {expiry_datetime.strftime(r'%y-%m-%d')}".lower(),
        "expiry": expiry_datetime,
        "multiplier": multiplier,
        "product_symbol": product_symbol.lower(),
        "display_name": display_name,
        "settlement": settlement,
    }
    return future_params


def generate_future_price_feeds_params(
    base_feed_id: str, base_feed_origin: str
) -> List[Dict[str, Any]]:
    price_feeds = [
        {
            "feed_id": base_feed_id,
            "origin": base_feed_origin,
            "delayed": False,
            "subscribe": True,
            "store_future_close_prices": False,
            "store_option_close_prices": False,
        },
        {
            "feed_id": "SPREAD_RELATIVE_TO_3M",
            "origin": "local",
            "delayed": False,
            "subscribe": True,
            "store_future_close_prices": False,
            "store_option_close_prices": False,
        },
    ]
    return price_feeds


def generate_future_price_feed_associations_params(
    future_symbol: str, base_feed_id: str, base_feed_origin: str
) -> List[Dict[str, Any]]:
    future_price_feed_associations = [
        {
            "future_symbol": future_symbol,
            "feed_id": base_feed_id,
            "feed_origin": base_feed_origin,
            "weight": 1,
        },
        {
            "future_symbol": future_symbol,
            "feed_id": "SPREAD_RELATIVE_TO_3M",
            "feed_origin": "local",
            "weight": 1,
        },
    ]
    return future_price_feed_associations


def generate_option_params(
    product_symbol: str,
    vol_surface_id: int,
    underlying_future_symbol: str,
    strike_intervals: List[List[int]],
    time_type: upeenums.TimeType,
    multiplier: int,
    vol_type: upeenums.VolType,
    expiry_datetime: datetime,
    display_name: Optional[str] = None,
) -> Dict[str, Any]:
    if expiry_datetime.tzinfo is None:
        raise ValueError(
            "Expiry for option was given without timezone: "
            f"{product_symbol} {expiry_datetime}"
        )
    option_params = {
        "symbol": f"{product_symbol} o {expiry_datetime.strftime(r'%y-%m-%d')} a".lower(),
        "product_symbol": product_symbol.lower(),
        "vol_surface_id": vol_surface_id,
        "underlying_future_symbol": underlying_future_symbol,
        "strike_intervals": strike_intervals,
        "time_type": time_type,
        "multiplier": multiplier,
        "vol_type": vol_type,
        "expiry": expiry_datetime,
        "display_name": display_name,
    }
    return option_params


def generate_vol_surface(
    model_type: str, expiry_datetime: datetime, params: Dict[str, Any]
) -> Dict[str, Any]:
    if expiry_datetime.tzinfo is None:
        raise ValueError(
            "Expiry for vol surface was given without timezone: "
            f"{model_type} {params} {expiry_datetime}"
        )
    vol_surface_params = {
        "model_type": model_type,
        "expiry": expiry_datetime,
        "params": params,
    }

    return vol_surface_params
