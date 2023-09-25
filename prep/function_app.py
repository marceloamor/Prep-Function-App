from redis.backoff import ExponentialBackoff
import azure.functions as func
from redis.retry import Retry
import sqlalchemy
import redis

import os


app = func.FunctionApp()

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_KEY"),
    ssl=True,
    retry=Retry(
        ExponentialBackoff(),
        10,
    ),
    retry_on_timeout=True,
)

sqlalchemy_pg_url = sqlalchemy.URL(
    "postgresql+psycopg",
    os.getenv("DB_SERVER_USERNAME"),
    os.getenv("DB_SERVER_PASSWORD"),
    os.getenv("DB_SERVER_HOST"),
    int(os.getenv("DB_SERVER_PORT")),
    os.getenv("DB_SERVER_DATABASE"),
    query={},
)

pg_engine = sqlalchemy.create_engine(sqlalchemy_pg_url, echo=False)
sessionmaker = sqlalchemy.orm.sessionmaker(pg_engine, expire_on_commit=False)

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "true").lower() in ("t", "true", "y", "1")
redis_key_append = ":dev" if USE_DEV_KEYS else ""

HEALTH_KEY = os.getenv("HEALTH_KEY") + redis_key_append
