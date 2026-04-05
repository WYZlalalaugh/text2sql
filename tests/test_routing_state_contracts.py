from __future__ import annotations

from importlib import import_module
from pathlib import Path
import sys
from typing import Any, cast

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "text2sql"))

graph_module = import_module("text2sql.graph")
state_module = import_module("text2sql.state")

IntentType = state_module.IntentType


def _recording_node(visited: list[str], name: str, payload: dict[str, object]) -> Any:
    def _node(_state: dict[str, object]) -> dict[str, object]:
        visited.append(name)
        return {**payload, "current_node": name}

    return _node


def test_value_query_routing_sequence(monkeypatch: Any) -> None:
    """Ensure VALUE_QUERY follows the preserved routing sequence."""
    visited: list[str] = []

    monkeypatch.setattr(
        graph_module,
        "create_intent_classifier",
        lambda *_args, **_kwargs: _recording_node(
            visited,
            "intent_classifier",
            {"intent_type": IntentType.VALUE_QUERY},
        ),
    )

    monkeypatch.setattr(
        graph_module,
        "create_ambiguity_checker",
        lambda *_args, **_kwargs: _recording_node(visited, "ambiguity_checker", {"ambiguity_detected": False}),
    )

    monkeypatch.setattr(
        graph_module,
        "create_query_planner",
        lambda *_args, **_kwargs: _recording_node(visited, "query_planner", {"query_plan": {}}),
    )

    monkeypatch.setattr(
        graph_module,
        "create_context_assembler",
        lambda *_args, **_kwargs: _recording_node(visited, "context_assembler", {"schema_context": "schema"}),
    )

    monkeypatch.setattr(graph_module, "create_sql_generator", lambda *_args, **_kwargs: _recording_node(visited, "sql_generator", {"generated_sql": "SELECT 1"}))
    monkeypatch.setattr(graph_module, "create_sql_executor", lambda *_args, **_kwargs: _recording_node(visited, "sql_executor", {"execution_result": [{"ok": True}]}))
    monkeypatch.setattr(graph_module, "create_response_generator", lambda *_args, **_kwargs: _recording_node(visited, "response_generator", {"final_response": "ok"}))

    # New iterative loop agents
    monkeypatch.setattr(graph_module, "create_metric_loop_planner", lambda *_args, **_kwargs: _recording_node(visited, "metric_loop_planner", {"loop_status": "completed", "loop_decision": {"decision": "complete"}}))
    monkeypatch.setattr(graph_module, "create_metric_sql_generator", lambda *_args, **_kwargs: _recording_node(visited, "metric_sql_generator", {"generated_sql": "SELECT 1"}))
    monkeypatch.setattr(graph_module, "create_metric_executor", lambda *_args, **_kwargs: _recording_node(visited, "metric_executor", {"step_results": {}}))
    monkeypatch.setattr(graph_module, "create_metric_observer", lambda *_args, **_kwargs: _recording_node(visited, "metric_observer", {}))

    app = graph_module.create_graph(llm_client=object())
    result = app.invoke(cast(dict[str, object], {"user_query": "count schools", "enable_suggestions": False}), config={"configurable": {"thread_id": "test-value-route"}})

    assert visited == ["intent_classifier", "query_planner", "context_assembler", "sql_generator", "sql_executor", "response_generator"]
    assert result["current_node"] == "response_generator"
    assert result.get("analysis_result") is None
    assert result.get("execution_path") is None


def test_metric_query_routing_hits_expected_nodes(monkeypatch: Any) -> None:
    """Assert METRIC_QUERY routes from ambiguity checker directly into iterative loop."""
    visited: list[str] = []

    monkeypatch.setattr(
        graph_module,
        "create_intent_classifier",
        lambda *_args, **_kwargs: _recording_node(visited, "intent_classifier", {"intent_type": IntentType.METRIC_QUERY}),
    )

    monkeypatch.setattr(graph_module, "create_ambiguity_checker", lambda *_args, **_kwargs: _recording_node(visited, "ambiguity_checker", {"ambiguity_detected": False}))

    manifest = {
        "summary_goal": "x",
        "steps": [
            {"type": "semantic_query"},
        ],
    }
    monkeypatch.setattr(graph_module, "create_query_planner", lambda *_args, **_kwargs: _recording_node(visited, "query_planner", {"query_manifest": manifest, "query_plan": manifest}))
    monkeypatch.setattr(graph_module, "create_context_assembler", lambda *_args, **_kwargs: _recording_node(visited, "context_assembler", {"schema_context": "schema"}))
    monkeypatch.setattr(graph_module, "create_metric_loop_planner", lambda *_args, **_kwargs: _recording_node(visited, "metric_loop_planner", {"loop_status": "completed", "loop_decision": {"decision": "complete", "reason": "done"}}))
    monkeypatch.setattr(graph_module, "create_response_generator", lambda *_args, **_kwargs: _recording_node(visited, "response_generator", {"final_response": "ok"}))

    app = graph_module.create_graph(llm_client=object())
    result = app.invoke(cast(dict[str, object], {"user_query": "metric check", "enable_suggestions": False}), config={"configurable": {"thread_id": "test-metric-route"}})

    assert visited == ["intent_classifier", "ambiguity_checker", "metric_loop_planner", "response_generator"]
    assert result["current_node"] == "response_generator"


def test_metric_query_legacy_fallback_path_remains_ართული(monkeypatch: Any) -> None:
    visited: list[str] = []

    monkeypatch.setattr(graph_module, "create_intent_classifier", lambda *_args, **_kwargs: _recording_node(visited, "intent_classifier", {"intent_type": IntentType.METRIC_QUERY}))
    monkeypatch.setattr(graph_module, "create_ambiguity_checker", lambda *_args, **_kwargs: _recording_node(visited, "ambiguity_checker", {"ambiguity_detected": False}))

    manifest = {"summary_goal": "semantic only", "steps": [{"type": "semantic_query"}]}
    monkeypatch.setattr(graph_module, "create_query_planner", lambda *_args, **_kwargs: _recording_node(visited, "query_planner", {"query_manifest": manifest, "query_plan": manifest}))
    monkeypatch.setattr(graph_module, "create_context_assembler", lambda *_args, **_kwargs: _recording_node(visited, "context_assembler", {"schema_context": "schema"}))
    monkeypatch.setattr(graph_module, "create_metric_loop_planner", lambda *_args, **_kwargs: _recording_node(visited, "metric_loop_planner", {"loop_status": "completed", "loop_decision": {"decision": "complete", "reason": "done"}}))
    monkeypatch.setattr(graph_module, "create_response_generator", lambda *_args, **_kwargs: _recording_node(visited, "response_generator", {"final_response": "ok"}))

    app = graph_module.create_graph(llm_client=object())
    result = app.invoke(cast(dict[str, object], {"user_query": "metric check", "enable_suggestions": False}), config={"configurable": {"thread_id": "test-metric-legacy-route"}})

    assert visited == ["intent_classifier", "ambiguity_checker", "metric_loop_planner", "response_generator"]
    assert "manifest_executor" not in visited
    assert "data_analyzer" not in visited
    assert result["current_node"] == "response_generator"


def test_shared_state_fields_exist_and_types() -> None:
    """Guard shared AgentState fields used by verifier/API for type invariants."""
    state_fields = state_module.AgentState.__annotations__

    expected = {
        "analysis_result": ("object",),
        "analysis_error": ("str", "None"),
        "verification_count": ("int",),
        # New iterative loop fields (replaced legacy fields)
        "loop_status": ("Literal", "planning", "executing"),
        "metric_plan_nodes": ("list",),
        "step_results": ("dict",),
    }

    for field, expects in expected.items():
        assert field in state_fields, f"Missing shared state field: {field}"
        ann = str(state_fields[field])
        assert any(e in ann for e in expects), f"Type annotation for {field} changed: {ann}"


def test_process_clarification_keeps_response_for_metric_route(monkeypatch: Any) -> None:
    """Ensure process_clarification keeps clarification_response through init for METRIC_QUERY."""
    visited: list[str] = []
    observed: dict[str, object] = {}

    monkeypatch.setattr(
        graph_module,
        "create_intent_classifier",
        lambda *_args, **_kwargs: _recording_node(visited, "intent_classifier", {"intent_type": IntentType.METRIC_QUERY}),
    )

    def _ambiguity_node(state: dict[str, object]) -> dict[str, object]:
        visited.append("ambiguity_checker")
        observed["clarification_response"] = state.get("clarification_response")
        return {
            "ambiguity_detected": False,
            "current_node": "ambiguity_checker",
        }

    monkeypatch.setattr(graph_module, "create_ambiguity_checker", lambda *_args, **_kwargs: _ambiguity_node)
    monkeypatch.setattr(graph_module, "create_metric_loop_planner", lambda *_args, **_kwargs: _recording_node(visited, "metric_loop_planner", {"loop_status": "completed", "loop_decision": {"decision": "complete"}}))
    monkeypatch.setattr(graph_module, "create_response_generator", lambda *_args, **_kwargs: _recording_node(visited, "response_generator", {"final_response": "ok"}))

    app = graph_module.create_graph(llm_client=object())
    result = graph_module.process_clarification(
        app,
        cast(dict[str, object], {
            "user_query": "metric check",
            "clarification_question": "请澄清统计口径",
            "clarification_count": 1,
            "messages": [],
        }),
        "按学校层级",
        config={"configurable": {"thread_id": "test-clarification-route"}},
    )

    assert observed.get("clarification_response") == "按学校层级"
    assert "query_planner" not in visited
    assert visited == ["intent_classifier", "ambiguity_checker", "metric_loop_planner", "response_generator"]
    assert result["current_node"] == "response_generator"
