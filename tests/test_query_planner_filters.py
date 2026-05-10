"""Query planner filter contract tests for METRIC_QUERY."""

from typing import cast

from state import IntentType
from agents.query_planner import (  # pyright: ignore[reportPrivateUsage]
    _get_plan_validation_error,
)


def test_metric_filter_node_requires_filters() -> None:
    plan = {
        "plan_nodes": [
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选",
                "required_tables": ["schools"],
                "depends_on": [],
                # 故意缺失 filters
            }
        ],
        "reasoning": "test",
    }

    err = _get_plan_validation_error(cast(dict[str, object], plan), IntentType.METRIC_QUERY)
    assert "filter 步骤缺少 filters" in err


def test_metric_filter_node_with_filters_is_valid() -> None:
    plan = {
        "plan_nodes": [
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选",
                "required_tables": ["schools"],
                "depends_on": [],
                "filters": [
                    {"field": "province", "operator": "like", "value": "北京"},
                ],
                "expected_outputs": ["school_id", "province"],
            },
            {
                "step_id": "s2",
                "intent_type": "aggregate",
                "description": "终局汇总",
                "required_tables": ["step_s1_output"],
                "depends_on": ["s1"],
                "expected_outputs": ["province", "school_count"],
            }
        ],
        "reasoning": "test",
    }

    err = _get_plan_validation_error(cast(dict[str, object], plan), IntentType.METRIC_QUERY)
    assert err == ""


def test_metric_non_filter_node_can_skip_filters() -> None:
    plan = {
        "plan_nodes": [
            {
                "step_id": "s2",
                "intent_type": "aggregate",
                "description": "聚合",
                "required_tables": ["step_s1_output"],
                "depends_on": ["s1"],
                "expected_outputs": ["province", "avg_score"],
            }
        ],
        "reasoning": "test",
    }

    err = _get_plan_validation_error(cast(dict[str, object], plan), IntentType.METRIC_QUERY)
    assert err == ""


def test_metric_plan_requires_terminal_aggregate_or_derive_step() -> None:
    plan = {
        "plan_nodes": [
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选",
                "required_tables": ["schools"],
                "depends_on": [],
                "filters": [{"field": "province", "operator": "like", "value": "京"}],
                "expected_outputs": ["school_id", "province"],
            },
            {
                "step_id": "s2",
                "intent_type": "join",
                "description": "关联",
                "required_tables": ["answers"],
                "depends_on": ["s1"],
                "expected_outputs": ["school_id", "score"],
            },
        ],
        "reasoning": "test",
    }

    err = _get_plan_validation_error(cast(dict[str, object], plan), IntentType.METRIC_QUERY)
    assert "缺少终局汇总步骤" in err


def test_metric_plan_terminal_aggregate_with_outputs_is_valid() -> None:
    plan = {
        "plan_nodes": [
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选",
                "required_tables": ["schools"],
                "depends_on": [],
                "filters": [{"field": "province", "operator": "like", "value": "京"}],
                "expected_outputs": ["school_id", "province"],
            },
            {
                "step_id": "s2",
                "intent_type": "aggregate",
                "description": "终局汇总",
                "required_tables": ["step_s1_output"],
                "depends_on": ["s1"],
                "expected_outputs": ["province", "avg_score", "comparison_label"],
            },
        ],
        "reasoning": "test",
    }

    err = _get_plan_validation_error(cast(dict[str, object], plan), IntentType.METRIC_QUERY)
    assert err == ""
