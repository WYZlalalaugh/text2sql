"""Regression test placed under text2sql/tests to satisfy tooling expectations.

Verifies legacy SQL-style list[dict] payloads pass through
normalize_canonical_tabular_result(...) and are JSON-safe for charting.
"""
from datetime import datetime, date
from decimal import Decimal
from importlib import import_module
from pathlib import Path
import sys
from typing import Callable, cast

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "text2sql"))

result_normalizer = import_module("text2sql.tools.result_normalizer")  # type: ignore
normalize_canonical_tabular_result: Callable[..., list[dict[str, object]]] = (
    cast(Callable[..., list[dict[str, object]]], result_normalizer.normalize_canonical_tabular_result)
)


def test_legacy_sql_list_of_dicts_passthrough_via_text2sql_tests_path():
    # Legacy SQL executor returned rows as list[dict]
    legacy_rows = [
        {
            "metric": "engagement",
            "value": Decimal("12.5"),
            "ts": datetime(2026, 3, 1, 12, 0, 0),
            "day": date(2026, 3, 1),
        },
        {
            "metric": "retention",
            "value": Decimal("7.0"),
            "ts": datetime(2026, 3, 2, 0, 0, 0),
            "day": date(2026, 3, 2),
        },
    ]

    normalized = normalize_canonical_tabular_result(
        payload=None, analysis_result=None, execution_result=legacy_rows
    )

    assert isinstance(normalized, list)
    assert len(normalized) == 2

    first: dict[str, object] = normalized[0]
    assert first["metric"] == "engagement"
    # Decimal -> float
    assert isinstance(first["value"], float) and first["value"] == 12.5
    # datetime -> ISO datetime string with T
    assert isinstance(first["ts"], str) and "T" in first["ts"] and first["ts"].startswith("2026-03-01T")
    # date -> ISO date string (no T)
    assert isinstance(first["day"], str) and first["day"] == "2026-03-01"

    second: dict[str, object] = normalized[1]
    assert second["metric"] == "retention"
    assert isinstance(second["value"], float) and second["value"] == 7.0
    assert isinstance(second["ts"], str) and second["ts"].startswith("2026-03-02T")
    assert isinstance(second["day"], str) and second["day"] == "2026-03-02"
