import httpx
import respx

from orchestrator.resources.pageviews_client import WikiPageViewsAPIClient

SAMPLE_RESPONSE = {
    "pageid": 534366,
    "title": "Barack Obama",
    "description": "44th president of the United States",
    "extract": "Barack Hussein Obama II is an American politician...",
    "wikibase_item": "Q76",
    "type": "standard",
}

SAMPLE_METADATA_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/Barack_Obama"


class TestFetchArticleMetadata:
    @respx.mock
    def test_fetch_success(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get(SAMPLE_METADATA_URL).mock(return_value=httpx.Response(200, json=SAMPLE_RESPONSE))
        summaries = pageviews_api_client.fetch_articles_metadata(["Barack_Obama"])

        assert len(summaries) == 1
        assert summaries[0]["pageid"] == 534366
        assert summaries[0]["title"] == "Barack Obama"
        assert summaries[0]["description"] == "44th president of the United States"
        assert summaries[0]["wikibase_item"] == "Q76"
        assert summaries[0]["type"] == "standard"

    @respx.mock
    def test_404_excluded_from_results(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get("https://en.wikipedia.org/api/rest_v1/page/summary/Dummy_Article").mock(
            return_value=httpx.Response(404)
        )
        respx.get(SAMPLE_METADATA_URL).mock(return_value=httpx.Response(200, json=SAMPLE_RESPONSE))

        summaries = pageviews_api_client.fetch_articles_metadata(["Dummy_Article", "Barack_Obama"])

        assert len(summaries) == 1
        assert summaries[0]["title"] == "Barack Obama"

    def test_empty_articles_list(self, pageviews_api_client: WikiPageViewsAPIClient):
        summaries = pageviews_api_client.fetch_articles_metadata([])
        assert summaries == []

    @respx.mock
    def test_missing_optional_fields(self, pageviews_api_client: WikiPageViewsAPIClient):
        minimal_response = {
            "pageid": 12345,
            "title": "Test Article",
            "type": "standard",
        }
        respx.get("https://en.wikipedia.org/api/rest_v1/page/summary/Test_Article").mock(
            return_value=httpx.Response(200, json=minimal_response)
        )

        summaries = pageviews_api_client.fetch_articles_metadata(["Test_Article"])

        assert len(summaries) == 1
        assert summaries[0]["pageid"] == 12345
        assert summaries[0]["title"] == "Test Article"
        assert summaries[0]["description"] == ""
        assert summaries[0]["extract"] == ""
        assert summaries[0]["wikibase_item"] == ""

    @respx.mock
    def test_partial_batch_failure(self, pageviews_api_client: WikiPageViewsAPIClient):
        respx.get("https://en.wikipedia.org/api/rest_v1/page/summary/Dummy_Article").mock(
            return_value=httpx.Response(500)
        )
        respx.get(SAMPLE_METADATA_URL).mock(return_value=httpx.Response(200, json=SAMPLE_RESPONSE))

        summaries = pageviews_api_client.fetch_articles_metadata(["Dummy_Article", "Barack_Obama"])

        # first article exhausts retries and is excluded from results;
        # other article succeeds and is present in the results
        assert len(summaries) == 1
        assert summaries[0]["title"] == "Barack Obama"
