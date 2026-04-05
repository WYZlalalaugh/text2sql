import sys
from pathlib import Path
from typing import cast


TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_loop_planner import create_metric_loop_planner
from state import AgentState
def test_metric_log_contract_persists_final_result_static() -> None:
    logger_path = TESTS_ROOT / "tools" / "logger.py"
    api_path = TESTS_ROOT / "api.py"

    logger_text = logger_path.read_text(encoding="utf-8")
    api_text = api_path.read_text(encoding="utf-8")

    assert "metric_final_result: Optional[Any] = None" in logger_text
    assert '"final_result": _ensure_serializable(metric_final_result)' in logger_text
    assert 'metric_final_result=state.get("execution_result")' in api_text


def test_metric_loop_planner_selects_next_runnable_step_by_depends_on() -> None:
    planner = create_metric_loop_planner()
    state = cast(
        AgentState,
        cast(
            object,
            {
                "planner_observations": [{"step_id": "s1", "observation_type": "success"}],
                "metric_plan_nodes": [
                    {"step_id": "s1", "depends_on": []},
                    {"step_id": "s3", "depends_on": ["s2"]},
                    {"step_id": "s2", "depends_on": ["s1"]},
                ],
                "current_step_id": "s1",
                "step_status_map": {"s1": "succeeded"},
                "step_results": {"s1": {"status": "success", "row_count": 10}},
                "loop_iteration": 1,
                "retry_counters": {},
            },
        ),
    )

    result = planner(state)

    assert result["current_step_id"] == "s2"
    assert result["loop_status"] == "executing"
    decision = cast(dict[str, object], result["loop_decision"])
    assert decision["decision"] == "continue"
    assert decision["next_step_id"] == "s2"
