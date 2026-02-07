import datetime

import httpx
import pytest
import respx

from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient

SAMPLE_DATE = datetime.datetime(year=2026, month=2, day=1)

SAMPLE_RESPONSE = {
    "items": [
        {
            "project": "en.wikipedia",
            "access": "all-access",
            "year": str(SAMPLE_DATE.year),
            "month": f"{SAMPLE_DATE.month}:02d",
            "day": f"{SAMPLE_DATE.day}:02d",
            "articles": [
                {"article": "Main_Page", "views": 5000000, "rank": 1},
                {"article": "Barack_Obama", "views": 245123, "rank": 2},
                {"article": "Special:Search", "views": 200000, "rank": 3},
                {"article": "Python_(programming_language)", "views": 150000, "rank": 4},
            ],
        }
    ]
}

TOP_ARTICLES_URL = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/"
    f"{SAMPLE_DATE.year}/{SAMPLE_DATE.month:02d}/{SAMPLE_DATE.day:02d}"
)


@pytest.mark.unit
class TestFetchTopArticles:
    @respx.mock
    def test_fetch_success(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(return_value=httpx.Response(200, json=SAMPLE_RESPONSE))
        articles = pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))

        assert len(articles) == 4
        assert articles[0]["article"] == "Main_Page"
        assert articles[0]["views"] == 5000000
        assert articles[0]["rank"] == 1

    @respx.mock
    def test_correct_headers_usage(self, pageviews_api_client: WikiPageViewsAPIClient):
        route = respx.get(TOP_ARTICLES_URL).mock(
            return_value=httpx.Response(200, json=SAMPLE_RESPONSE)
        )
        pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))

        assert route.calls[0].request.headers["user-agent"] == "WikiPulse-Test/1.0"

    @respx.mock
    def test_articles_response_schema(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(return_value=httpx.Response(200, json=SAMPLE_RESPONSE))

        articles = pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))

        for article in articles:
            assert set(article.keys()) == {"article", "views", "rank"}
            assert isinstance(article["article"], str)
            assert isinstance(article["views"], int)
            assert isinstance(article["rank"], int)

    @respx.mock
    def test_404_raises_runtime_error(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(return_value=httpx.Response(404))

        with pytest.raises(RuntimeError, match="No articles found"):
            pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))

    @respx.mock
    def test_429_forces_retry(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(200, json=SAMPLE_RESPONSE),
            ]
        )

        articles = pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))
        assert len(articles) == 4

    @respx.mock
    def test_500_forces_retry(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(
            side_effect=[
                httpx.Response(500),
                httpx.Response(200, json=SAMPLE_RESPONSE),
            ]
        )

        articles = pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))
        assert len(articles) == 4

    @respx.mock
    def test_retry_exhaustion_raises_error(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(TOP_ARTICLES_URL).mock(return_value=httpx.Response(500))

        with pytest.raises(RuntimeError, match="Max retries"):
            pageviews_api_client.fetch_top_articles(SAMPLE_DATE.strftime("%Y-%m-%d"))
