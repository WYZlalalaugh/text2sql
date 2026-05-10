"""
Baseline metric corpus tests for parity comparison.

This module validates the baseline metric output structure
for use in migration parity testing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict, cast

import pytest


# Path to baseline corpus fixture
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "metric_baseline_corpus.json"


class BaselineQuery(TypedDict):
    query_id: str
    description: str
    user_query: str
    intent_type: str
    expected_schema: dict[str, object]


class BaselineCorpus(TypedDict):
    version: str
    captured_at: str
    description: str
    schema_version: str
    output_fields: list[str]
    queries: list[BaselineQuery]


class TestMetricBaselineCorpus:
    """Tests for baseline metric corpus structure."""

    @pytest.fixture
    def baseline_corpus(self) -> BaselineCorpus:
        """Load the baseline corpus fixture."""
        with open(FIXTURE_PATH, encoding="utf-8") as f:
            return cast(BaselineCorpus, json.load(f))

    def test_corpus_version_exists(self, baseline_corpus: BaselineCorpus) -> None:
        """Corpus must have a version field."""
        assert "version" in baseline_corpus
        assert baseline_corpus["version"] == "1.0.0"

    def test_corpus_has_description(self, baseline_corpus: BaselineCorpus) -> None:
        """Corpus must have a description."""
        assert "description" in baseline_corpus
        assert "baseline" in baseline_corpus["description"].lower()

    def test_corpus_has_output_fields(self, baseline_corpus: BaselineCorpus) -> None:
        """Corpus must define expected output fields."""
        assert "output_fields" in baseline_corpus
        fields = baseline_corpus["output_fields"]

        # Critical fields for parity comparison
        critical_fields = [
            "analysis_result",
            "analysis_error",
            "verification_passed",
            "execution_path",
            "legacy_fallback_triggered",
        ]
        for field in critical_fields:
            assert field in fields, f"Missing critical field: {field}"

    def test_corpus_has_queries(self, baseline_corpus: BaselineCorpus) -> None:
        """Corpus must have example queries."""
        assert "queries" in baseline_corpus
        assert len(baseline_corpus["queries"]) >= 3

    def test_query_has_required_fields(self, baseline_corpus: BaselineCorpus) -> None:
        """Each query must have required fields."""
        for query in baseline_corpus["queries"]:
            assert "query_id" in query
            assert "user_query" in query
            assert "intent_type" in query
            assert query["intent_type"] == "METRIC_QUERY"
            assert "expected_schema" in query
            assert isinstance(query["expected_schema"], dict)

    def test_query_expected_schema_keys_are_known_output_fields(self, baseline_corpus: BaselineCorpus) -> None:
        """Each query schema should only use known output fields."""
        output_fields: set[str] = set(baseline_corpus["output_fields"])
        for query in baseline_corpus["queries"]:
            schema: dict[str, object] = query["expected_schema"]
            assert set(schema).issubset(output_fields), f"Unknown schema keys in {query['query_id']}: {sorted(set(schema) - output_fields)}"

    def test_query_ids_are_unique(self, baseline_corpus: BaselineCorpus) -> None:
        """Query IDs must be unique."""
        ids: list[str] = [q["query_id"] for q in baseline_corpus["queries"]]
        assert len(ids) == len(set(ids)), f"Duplicate query IDs: {ids}"


class TestBaselineParityHelper:
    """Helper utilities for parity testing."""

    @pytest.fixture
    def baseline_corpus(self) -> BaselineCorpus:
        """Load the baseline corpus fixture."""
        with open(FIXTURE_PATH, encoding="utf-8") as f:
            return cast(BaselineCorpus, json.load(f))

    def test_can_extract_output_field_list(self, baseline_corpus: BaselineCorpus) -> None:
        """Helper can extract the list of fields to compare."""
        fields: list[str] = baseline_corpus["output_fields"]
        assert isinstance(fields, list)
        assert len(fields) > 0

    def test_can_find_query_by_id(self, baseline_corpus: BaselineCorpus) -> None:
        """Helper can find a specific query by ID."""
        query_map: dict[str, BaselineQuery] = {q["query_id"]: q for q in baseline_corpus["queries"]}

        # Should have at least these queries
        expected_ids = ["metric_agg_simple", "metric_filter_agg"]
        for qid in expected_ids:
            assert qid in query_map, f"Missing expected query: {qid}"


def get_baseline_output_fields() -> list[str]:
    """
    Get the list of output fields to compare for parity testing.

    Returns:
        List of field names that should be present in metric query outputs.
    """
    with open(FIXTURE_PATH, encoding="utf-8") as f:
        corpus = cast(BaselineCorpus, json.load(f))
    return corpus["output_fields"]


def get_baseline_queries() -> list[BaselineQuery]:
    """
    Get the list of baseline queries for parity testing.

    Returns:
        List of query definitions with expected schemas.
    """
    with open(FIXTURE_PATH, encoding="utf-8") as f:
        corpus = cast(BaselineCorpus, json.load(f))
    return corpus["queries"]
