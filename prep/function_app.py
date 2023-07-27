from redis.backoff import ExponentialBackoff
import azure.functions as func
from redis.retry import Retry
import sqlalchemy
import redis

import os


app = func.FunctionApp()


redis_host = os.getenv("REDIS_HOST")
redis_port = int(os.getenv("REDIS_PORT"))
redis_key = os.getenv("REDIS_KEY")

redis_conn = redis.Redis(
    host=redis_host,
    port=redis_port,
    password=redis_key,
    ssl=True,
    retry=Retry(
        ExponentialBackoff(),
        10,
    ),
    retry_on_timeout=True,
)

pg_db_server_host = os.getenv("DB_SERVER_HOST")
pg_db_server_port = int(os.getenv("DB_SERVER_PORT"))
pg_db_server_username = os.getenv("DB_SERVER_USERNAME")
pg_db_server_password = os.getenv("DB_SERVER_PASSWORD")
pg_db_server_database = os.getenv("DB_SERVER_DATABASE")

sqlalchemy_pg_url = sqlalchemy.URL(
    "postgresql+psycopg",
    pg_db_server_username,
    pg_db_server_password,
    pg_db_server_host,
    pg_db_server_port,
    pg_db_server_database,
    query={},
)

pg_engine = sqlalchemy.create_engine(sqlalchemy_pg_url, echo=False)
sessionmaker = sqlalchemy.orm.sessionmaker(pg_engine, expire_on_commit=False)

USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "true").lower() in ("t", "true", "y", "1")
redis_key_append = ":dev" if USE_DEV_KEYS else ""

RJO_SFTP_HOST = os.getenv("RJO_SFTP_HOST")
RJO_SFTP_PORT = os.getenv("RJO_SFTP_PORT")
RJO_SFTP_USER = os.getenv("RJO_SFTP_USER")
RJO_SFTP_PASS = os.getenv("RJO_SFTP_PASS")

HEALTH_KEY = os.getenv("HEALTH_KEY") + redis_key_append
