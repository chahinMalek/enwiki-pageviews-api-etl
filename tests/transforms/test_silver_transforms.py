import datetime

import polars as pl
import pytest

from orchestrator.assets.silver import (
    transform_article_metadata,
    transform_daily_pageviews,
)

SAMPLE_DATE = datetime.date(year=2026, month=1, day=15)


@pytest.fixture
def bronze_pageviews() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "ingestion_date": [SAMPLE_DATE] * 9,
            "article": [
                "Main_Page",
                "Special:Search",
                "Wikipedia:Main_Page",
                "File:Example.jpg",
                "Portal:Current_events",
                "Category:Living_people",
                "Help:Contents",
                "Barack_Obama",
                "Python_(programming_language)",
            ],
            "views": [6_000_000, 500_000, 200_000, 100_000, 80_000, 60_000, 40_000, 50_000, 30_000],
            "rank": list(range(1, 10)),
        }
    )


@pytest.fixture
def bronze_article_meta() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pageid": [534366, 23862, 1],
            "title": ["Barack_Obama", "Python_(programming_language)", "Beyonc%C3%A9"],
            "description": ["44th U.S. president", "  programming language  ", "American singer"],
            "extract": ["Barack Obama is...", " Python is... ", "Beyoncé is..."],
            "wikibase_item": ["Q76", "Q28865", "Q36153"],
            "type": ["standard", "standard", "standard"],
            "first_seen_date": [SAMPLE_DATE] * 3,
        }
    )


class TestTransformDailyPageviews:
    def test_filters_all_non_article_namespaces(self, bronze_pageviews):
        result = transform_daily_pageviews(bronze_pageviews)

        titles = set(result["article"].to_list())
        assert titles == {"Barack Obama", "Python (programming language)"}

    def test_case_insensitive_namespace_filtering(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 3,
                "article": ["SPECIAL:Search", "wikipedia:About", "Barack_Obama"],
                "views": [100, 100, 50_000],
                "rank": [1, 2, 3],
            }
        )
        result = transform_daily_pageviews(df)
        assert result["article"].to_list() == ["Barack Obama"]

    def test_exact_match_does_not_over_filter(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 2,
                "article": ["Main_Page", "Main_Page_Redesign"],
                "views": [6_000_000, 5_000],
                "rank": [1, 2],
            }
        )
        result = transform_daily_pageviews(df)
        assert "Main Page Redesign" in result["article"].to_list()
        assert "Main Page" not in result["article"].to_list()

    def test_url_decodes_underscores_and_percent_encoding(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 3,
                "article": ["Barack_Obama", "Beyonc%C3%A9", "Rock_%26_Roll"],
                "views": [50_000, 40_000, 30_000],
                "rank": [1, 2, 3],
            }
        )
        result = transform_daily_pageviews(df)
        titles = result["article"].to_list()
        assert "Barack Obama" in titles
        assert "Beyoncé" in titles
        assert "Rock & Roll" in titles

    def test_filters_zero_views(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 2,
                "article": ["Barack_Obama", "Some_Article"],
                "views": [50_000, 0],
                "rank": [1, 2],
            }
        )
        result = transform_daily_pageviews(df)
        assert len(result) == 1
        assert result["article"].to_list() == ["Barack Obama"]

    def test_filters_null_titles(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 2,
                "article": ["Barack_Obama", None],
                "views": [50_000, 10_000],
                "rank": [1, 2],
            }
        )
        result = transform_daily_pageviews(df)
        assert len(result) == 1

    def test_deduplicates_keeping_highest_views(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 2,
                "article": ["Barack_Obama", "Barack_Obama"],
                "views": [30_000, 50_000],
                "rank": [2, 1],
            }
        )
        result = transform_daily_pageviews(df)
        assert len(result) == 1
        assert result["views"].to_list() == [50_000]

    def test_sorted_by_rank(self, bronze_pageviews):
        result = transform_daily_pageviews(bronze_pageviews)
        ranks = result["rank"].to_list()
        assert ranks == sorted(ranks)

    def test_preserves_schema(self):
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE],
                "article": ["Barack_Obama"],
                "views": [50_000],
                "rank": [1],
            }
        )
        result = transform_daily_pageviews(df)
        assert set(result.columns) == {"ingestion_date", "article", "views", "rank"}
        assert result.schema["ingestion_date"] == pl.Date
        assert result.schema["article"] == pl.Utf8
        assert result.schema["views"] == pl.Int64
        assert result.schema["rank"] == pl.Int64

    def test_empty_after_all_filtered(self):
        # if every row is a non-article, output is empty
        df = pl.DataFrame(
            {
                "ingestion_date": [SAMPLE_DATE] * 2,
                "article": ["Main_Page", "Special:Search"],
                "views": [6_000_000, 500_000],
                "rank": [1, 2],
            }
        )
        result = transform_daily_pageviews(df)
        assert len(result) == 0


class TestTransformArticleMetadata:
    def test_url_decodes_titles(self, bronze_article_meta):
        result = transform_article_metadata(bronze_article_meta)
        titles = result["title"].to_list()
        assert "Barack Obama" in titles
        assert "Python (programming language)" in titles
        assert "Beyoncé" in titles

    def test_strips_whitespace_from_text_fields(self, bronze_article_meta):
        result = transform_article_metadata(bronze_article_meta)
        row = result.filter(pl.col("title") == "Python (programming language)")
        assert row["description"].to_list() == ["programming language"]
        assert row["extract"].to_list() == ["Python is..."]

    def test_filters_null_pageid(self):
        df = pl.DataFrame(
            {
                "pageid": [1, None],
                "title": ["Good", "NullId"],
                "description": ["d", "d"],
                "extract": ["e", "e"],
                "wikibase_item": ["Q1", "Q2"],
                "type": ["standard", "standard"],
                "first_seen_date": [SAMPLE_DATE] * 2,
            }
        )
        result = transform_article_metadata(df)
        assert len(result) == 1
        assert result["title"].to_list() == ["Good"]

    def test_filters_null_title(self):
        df = pl.DataFrame(
            {
                "pageid": [1, 2],
                "title": ["Good", None],
                "description": ["d", "d"],
                "extract": ["e", "e"],
                "wikibase_item": ["Q1", "Q2"],
                "type": ["standard", "standard"],
                "first_seen_date": [SAMPLE_DATE] * 2,
            }
        )
        result = transform_article_metadata(df)
        assert len(result) == 1

    def test_deduplicates_by_pageid(self):
        df = pl.DataFrame(
            {
                "pageid": [1, 1],
                "title": ["First", "Second"],
                "description": ["d", "d2"],
                "extract": ["e", "e2"],
                "wikibase_item": ["Q1", "Q1"],
                "type": ["standard", "standard"],
                "first_seen_date": [SAMPLE_DATE] * 2,
            }
        )
        result = transform_article_metadata(df)
        assert len(result) == 1

    def test_preserves_schema(self, bronze_article_meta):
        result = transform_article_metadata(bronze_article_meta)
        expected_cols = {
            "pageid",
            "title",
            "description",
            "extract",
            "wikibase_item",
            "type",
            "first_seen_date",
        }
        assert set(result.columns) == expected_cols
