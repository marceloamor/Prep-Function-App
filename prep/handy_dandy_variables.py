import os


USE_DEV_KEYS = os.getenv("USE_DEV_KEYS", "true").lower() in (
    "t",
    "true",
    "y",
    "yes",
    "1",
)
redis_key_append = ":dev" if USE_DEV_KEYS else ""

HEALTH_KEY = os.getenv("HEALTH_KEY", "prep:health")
