import os
from pathlib import Path

DATA_DIR = Path("data")
DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt_src"


def get_pg_connection_params() -> dict:
    """Return psycopg2 connection parameters from environment variables."""
    return {
        "user": os.environ.get("POSTGRES_USER", "wikipulse"),
        "password": os.environ.get("POSTGRES_PASSWORD", "wikipulse"),
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "wikipulse"),
    }


def get_metabase_config() -> dict:
    """Read Metabase connection settings from environment variables."""
    return {
        "metabase_url": os.environ.get("METABASE_URL", "http://metabase:3000"),
        "metabase_email": os.environ.get("METABASE_EMAIL", "admin@wikipulse.dev"),
        "metabase_password": os.environ.get("METABASE_PASSWORD", "W1k!Pulse2026"),
        "request_timeout_seconds": float(os.environ.get("METABASE_REQUEST_TIMEOUT", "30")),
        "startup_max_wait_seconds": float(os.environ.get("METABASE_STARTUP_TIMEOUT", "120")),
        "startup_poll_interval_seconds": float(
            os.environ.get("METABASE_STARTUP_POLL_INTERVAL", "5")
        ),
    }


def get_pageviews_config() -> dict:
    """Read PageViews client configs from environment variables."""
    return {
        "max_concurrent_requests": int(os.environ.get("PAGEVIEWS_CONCURRENCY", "4")),
        "max_retries": int(os.environ.get("PAGEVIEWS_MAX_RETRIES", "5")),
    }
