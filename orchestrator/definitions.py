from dagster import Definitions, asset


@asset
def hello_wikipulse() -> str:
    return "WikiPulse is ready!"


defs = Definitions(
    assets=[hello_wikipulse],
)
