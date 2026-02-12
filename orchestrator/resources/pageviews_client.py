import asyncio
import datetime
import random
import time

import httpx
from dagster import ConfigurableResource, get_dagster_logger


class WikiPageViewsAPIClient(ConfigurableResource):
    """
    Dagster resource for interacting with Wikimedia PageViews APIs.

    Features:
    - Sync httpx.Client for single-request endpoints (top articles)
    - Async httpx.AsyncClient with semaphore rate limiting for batch endpoints (article metadata)
    - Exponential backoff with jitter on retryable errors
    """

    user_agent: str = "WikiPulse/1.0 (contact@email.com)"
    max_concurrent_requests: int = 4
    max_retries: int = 5
    base_backoff_seconds: float = 2.0
    request_timeout_seconds: float = 30.0

    def fetch_top_articles(self, date_str: str) -> list[dict]:
        """
        Fetch the top viewed articles for a given date.

        Args:
            date_str: Date in 'YYYY-MM-DD' format (from Dagster partition key).

        Returns:
            List of dicts with keys: article, views, rank.
        """
        date_obj = datetime.date.fromisoformat(date_str)
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/"
            f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
        )

        with httpx.Client(
            headers={"User-Agent": self.user_agent},
            timeout=self.request_timeout_seconds,
        ) as client:
            data = self._request_with_retry(client, url)

        if data is None:
            raise RuntimeError(f"No articles found for date {date_str}")

        articles = data["items"][0]["articles"]
        return [
            {
                "article": article["article"],
                "views": article["views"],
                "rank": article["rank"],
            }
            for article in articles
        ]

    def fetch_articles_metadata(self, titles: list[str]) -> list[dict]:
        """
        Fetch metadata for a batch of article titles.

        Args:
            titles: List of URL-encoded article titles (as from bronze_daily_top).

        Returns:
            List of dicts with metadata fields. Titles that 404 are excluded.
        """
        if not titles:
            return []
        return asyncio.run(self._fetch_articles_metadata_async(titles))

    async def _fetch_articles_metadata_async(self, titles: list[str]) -> list[dict]:
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        logger = get_dagster_logger()

        async with httpx.AsyncClient(
            headers={"User-Agent": self.user_agent},
            timeout=self.request_timeout_seconds,
        ) as client:
            tasks = [self._fetch_metadata(client, semaphore, title) for title in titles]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        summaries = []
        for title, result in zip(titles, results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch summary for '{title}': {result}")
                continue
            if result is not None:
                summaries.append(result)

        return summaries

    async def _fetch_metadata(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        title: str,
    ) -> dict | None:
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
        data = await self._arequest_with_retry(client, semaphore, url)

        if data is None:
            return None

        return {
            "pageid": data.get("pageid"),
            "title": data.get("title", ""),
            "description": data.get("description", ""),
            "extract": data.get("extract", ""),
            "wikibase_item": data.get("wikibase_item", ""),
            "type": data.get("type", ""),
        }

    def _backoff(self, attempt: int) -> float:
        backoff = self.base_backoff_seconds * (2**attempt)
        jitter = random.uniform(0, backoff * 0.5)
        return backoff + jitter

    def _request_with_retry(self, client: httpx.Client, url: str) -> dict | None:
        """
        Synchronous HTTP GET with exponential backoff.

        Returns parsed JSON dict on success, None on 404.
        Raises RuntimeError after max_retries exhausted on retryable errors.
        """
        logger = get_dagster_logger()

        for attempt in range(self.max_retries):
            try:
                response = client.get(url)

                if response.status_code == 200:
                    return response.json()

                if response.status_code == 404:
                    logger.warning(f"404 Not Found: {url}")
                    return None

                if response.status_code in (429, 500, 502, 503, 504):
                    wait_time = self._backoff(attempt)
                    logger.warning(
                        f"HTTP {response.status_code} on {url}, "
                        f"retry {attempt + 1}/{self.max_retries} in {wait_time:.1f}s"
                    )
                    time.sleep(wait_time)
                    continue

                # non-retryable error
                response.raise_for_status()

            except httpx.TimeoutException:
                wait_time = self._backoff(attempt)
                logger.warning(
                    f"Timeout on {url}, "
                    f"retry {attempt + 1}/{self.max_retries} in {wait_time:.1f}s"
                )
                time.sleep(wait_time)
                continue

        raise RuntimeError(f"Max retries ({self.max_retries}) exhausted for {url}")

    async def _arequest_with_retry(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        url: str,
    ) -> dict | None:
        """
        Async HTTP GET with semaphore rate limiting and exponential backoff.

        Returns parsed JSON dict on success, None on 404.
        Raises RuntimeError after max_retries exhausted on retryable errors.
        """
        logger = get_dagster_logger()

        for attempt in range(self.max_retries):
            async with semaphore:
                try:
                    response = await client.get(url)

                    if response.status_code == 200:
                        return response.json()

                    if response.status_code == 404:
                        logger.warning(f"404 Not Found: {url}")
                        return None

                    if response.status_code in (429, 500, 502, 503, 504):
                        wait_time = self._backoff(attempt)
                        logger.warning(
                            f"HTTP {response.status_code} on {url}, "
                            f"retry {attempt + 1}/{self.max_retries} in {wait_time:.1f}s"
                        )
                        await asyncio.sleep(wait_time)
                        continue

                    # non-retryable error
                    response.raise_for_status()

                except httpx.TimeoutException:
                    wait_time = self._backoff(attempt)
                    logger.warning(
                        f"Timeout on {url}, "
                        f"retry {attempt + 1}/{self.max_retries} in {wait_time:.1f}s"
                    )
                    await asyncio.sleep(wait_time)
                    continue

        raise RuntimeError(f"Max retries ({self.max_retries}) exhausted for {url}")
