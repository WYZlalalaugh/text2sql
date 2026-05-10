"""
指标循环规划器测试
"""
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_loop_planner import (
    create_metric_loop_planner,
    _build_execution_history,  # pyright: ignore[reportPrivateUsage]
)
from state import AgentState


class TestMetricLoopPlanner:
    """测试指标循环规划器"""

    def test_initial_planning_selects_first_step(self):
        """初始状态应选择第一个步骤继续执行"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "loop_status": "planning",
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "loop_iteration": 0,
                    "retry_counters": {},
                },
            ),
        )

        result = planner(state)

        assert result["current_node"] == "metric_loop_planner"
        assert result["current_step_id"] == "s1"
        assert result["loop_status"] == "executing"
        assert result["loop_iteration"] == 1
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "continue"
        assert decision["next_step_id"] == "s1"

    def test_continue_to_next_step_on_success(self):
        """成功观察应推进到下一个步骤"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s1", "observation_type": "success"}],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "current_step_id": "s1",
                    "loop_iteration": 1,
                    "retry_counters": {},
                },
            ),
        )

        result = planner(state)

        assert result["current_step_id"] == "s2"
        assert result["loop_status"] == "executing"
        assert result["loop_iteration"] == 2
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "continue"
        assert decision["next_step_id"] == "s2"

    def test_adjust_retry_on_failed_observation(self):
        """失败观察且未超重试次数应返回adjust并累加计数"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s1", "observation_type": "failed"}],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "retry_counters": {"s1": 1},
                    "loop_iteration": 2,
                },
            ),
        )

        result = planner(state)

        assert result["loop_status"] == "adjusting"
        assert result["loop_iteration"] == 3
        assert cast(dict[str, int], result["retry_counters"])["s1"] == 2
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "adjust"
        assert decision["next_step_id"] == "s1"

    def test_fail_on_max_retries(self):
        """达到每步最大重试次数应失败"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s1", "observation_type": "failed"}],
                    "metric_plan_nodes": [{"step_id": "s1"}],
                    "retry_counters": {"s1": 3},
                    "loop_iteration": 3,
                },
            ),
        )

        result = planner(state)

        assert result["loop_status"] == "failed"
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "fail"
        assert "最大重试次数" in str(decision["reason"])

    def test_fail_on_max_iterations(self):
        """达到总循环最大迭代次数应失败"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s1", "observation_type": "success"}],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "loop_iteration": 20,
                },
            ),
        )

        result = planner(state)

        assert result["loop_status"] == "failed"
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "fail"
        assert decision["reason"] == "达到最大迭代次数"

    def test_complete_when_all_steps_done(self):
        """最后一步成功后应返回complete"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s2", "observation_type": "success"}],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "current_step_id": "s2",
                    "loop_iteration": 4,
                },
            ),
        )

        result = planner(state)

        assert result["loop_status"] == "completed"
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["decision"] == "complete"
        assert "所有步骤执行成功" in str(decision["reason"])

    def test_retry_path_sets_current_step_id_without_llm(self):
        """无LLM时失败重试也必须设置 current_step_id。"""
        planner = create_metric_loop_planner()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [{"step_id": "s3", "observation_type": "failed"}],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s3"}],
                    "retry_counters": {"s3": 0},
                    "loop_iteration": 2,
                },
            ),
        )

        result = planner(state)

        assert result["loop_status"] == "adjusting"
        assert result["current_step_id"] == "s3"
        decision = cast(dict[str, object], result["loop_decision"])
        assert decision["next_step_id"] == "s3"

    def test_execution_history_keeps_failed_attempts(self):
        """执行历史摘要应保留同一步骤的失败尝试，不被覆盖。"""
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "metric_plan_nodes": [
                        {"step_id": "s1", "intent_type": "filter", "description": "step1"},
                        {"step_id": "s2", "intent_type": "derive", "description": "step2"},
                    ],
                    "step_results": {
                        "s1": {"status": "success", "row_count": 10},
                        "s2": {"status": "success", "row_count": 3},
                    },
                    "execution_history": [
                        {"step_id": "s1", "status": "success"},
                        {"step_id": "s2", "status": "failed", "error": "Unknown column x"},
                        {"step_id": "s2", "status": "failed", "error": "Unknown column y"},
                        {"step_id": "s2", "status": "success"},
                    ],
                },
            ),
        )

        observation = cast(dict[str, object], {"error_summary": "Unknown column y"})
        text = _build_execution_history(state, "s2", observation)

        assert "s2#1" in text
        assert "s2#2" in text
        assert "Unknown column x" in text
        assert "Unknown column y" in text

    def test_llm_adjust_retry_keeps_failed_step_id_stable(self):
        """LLM重规划即使输出 s6_adjust，也必须稳定回写为 s6。"""

        class FakeLLM:
            def invoke(self, prompt: str) -> object:
                _ = prompt
                return (
                    '{"plan_nodes": ['
                    '{"step_id":"s5","intent_type":"aggregate","depends_on":[]},'
                    '{"step_id":"s6_adjust","intent_type":"derive","depends_on":["s5"]},'
                    '{"step_id":"s7","intent_type":"aggregate","depends_on":["s6_adjust"]}'
                    ']}'
                )

        planner = create_metric_loop_planner(FakeLLM())
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "planner_observations": [
                        {
                            "step_id": "s6",
                            "observation_type": "failed",
                            "error_category": "sql_error",
                            "error_summary": "Unknown column",
                            "sql_executed": "SELECT * FROM t",
                        }
                    ],
                    "metric_plan_nodes": [
                        {"step_id": "s5", "intent_type": "aggregate"},
                        {"step_id": "s6", "intent_type": "derive", "depends_on": ["s5"]},
                        {"step_id": "s7", "intent_type": "aggregate", "depends_on": ["s6"]},
                    ],
                    "retry_counters": {"s6": 0},
                    "loop_iteration": 2,
                    "user_query": "test",
                    "metrics_context": "{}",
                    "schema_context": "{}",
                },
            ),
        )

        result = planner(state)

        assert result["current_step_id"] == "s6"
        nodes = cast(list[dict[str, object]], result["metric_plan_nodes"])
        step_ids = [str(node.get("step_id")) for node in nodes]
        assert "s6" in step_ids
        assert "s6_adjust" not in step_ids

        s7_node = next(node for node in nodes if str(node.get("step_id")) == "s7")
        assert s7_node.get("depends_on") == ["s6"]
