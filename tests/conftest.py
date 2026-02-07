import pytest

from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient


@pytest.fixture
def pageviews_api_client() -> WikiPageViewsAPIClient:
    return WikiPageViewsAPIClient(
        user_agent="WikiPulse-Test/1.0",
        max_concurrent_requests=5,
        max_retries=3,
        base_backoff_seconds=0.01,
        request_timeout_seconds=5.0,
    )
