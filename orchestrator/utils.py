import os
from urllib.parse import unquote


def decode_title(title: str) -> str:
    """URL-decode a Wikipedia article title and replace underscores with spaces."""
    return unquote(title).replace("_", " ")


def get_pg_connection_params() -> dict:
    """Return psycopg2 connection parameters from environment variables."""
    return {
        "user": os.environ.get("POSTGRES_USER", "wikipulse"),
        "password": os.environ.get("POSTGRES_PASSWORD", "wikipulse"),
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "dbname": os.environ.get("POSTGRES_DB", "wikipulse"),
    }


def get_pg_connection_string() -> str:
    """Return a SQLAlchemy connection string from environment variables."""
    params = get_pg_connection_params()
    return (
        f"postgresql+psycopg2://{params['user']}:{params['password']}"
        f"@{params['host']}:{params['port']}/{params['dbname']}"
    )
