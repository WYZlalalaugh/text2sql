"""
指标观察器测试
"""
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_observer import create_metric_observer
from state import AgentState


class TestMetricObserver:
    """测试指标观察器"""

    def test_success_observation_generation(self):
        """成功执行应生成success观察"""
        observer = create_metric_observer()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "generated_sql": "SELECT school_id, score FROM tmp_scores",
                    "step_results": {
                        "s1": {
                            "row_count": 12,
                            "execution_time_ms": 35,
                            "sample_rows": [{"school_id": 1, "score": 88.5}],
                            "schema_snapshot": {"school_id": "int", "score": "double"},
                        }
                    },
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}],
                    "loop_iteration": 0,
                },
            ),
        )

        result = observer(state)

        assert result["current_node"] == "metric_observer"
        assert result["loop_status"] == "completed"
        observations = cast(list[dict[str, object]], result["planner_observations"])
        assert len(observations) == 1

        observation = observations[0]
        assert observation["observation_type"] == "success"
        assert observation["error_summary"] is None
        data_summary = cast(dict[str, object], observation["data_summary"])
        assert data_summary["row_count"] == 12
        assert data_summary["schema_snapshot"] == {"school_id": "int", "score": "double"}

    def test_failure_observation_with_error_categorization(self):
        """执行失败应生成分类后的失败观察"""
        observer = create_metric_observer()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "generated_sql": "SELEC bad_sql",
                    "execution_error": "You have an SQL syntax error near 'SELEC'",
                    "step_results": {},
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}],
                    "loop_iteration": 1,
                },
            ),
        )

        result = observer(state)
        observations = cast(list[dict[str, object]], result["planner_observations"])
        observation = observations[0]

        assert observation["observation_type"] == "failed"
        assert observation["error_category"] == "SYNTAX_ERROR"
        assert observation["fix_suggestion"] == "使用更简单的SQL语法，避免复杂函数"
        assert observation["raw_error"] == "You have an SQL syntax error near 'SELEC'"
        assert result["loop_status"] == "adjusting"

    def test_failure_observation_normalizes_wrapped_error_prefix(self):
        """包装前缀（执行失败:）应被清理，避免错误摘要噪音。"""
        observer = create_metric_observer()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "generated_sql": "SELECT school_name FROM schools",
                    "execution_error": "执行失败: Unknown column 'school_name' in 'field list'",
                    "step_results": {},
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}],
                    "loop_iteration": 1,
                },
            ),
        )

        result = observer(state)
        observations = cast(list[dict[str, object]], result["planner_observations"])
        observation = observations[0]

        assert observation["error_category"] == "SCHEMA_MISMATCH"
        assert str(observation["error_summary"]).startswith("Unknown column")

    def test_loop_status_determination_for_next_step_and_max_iteration(self):
        """成功且有后续步骤时继续执行；达到最大迭代时失败"""
        observer = create_metric_observer()

        running_state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "generated_sql": "SELECT 1",
                    "step_results": {"s1": {"row_count": 1, "execution_time_ms": 10}},
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
                    "loop_iteration": 2,
                },
            ),
        )
        running_result = observer(running_state)
        assert running_result["loop_status"] == "executing"

        exhausted_state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s2",
                    "generated_sql": "SELECT 2",
                    "step_results": {"s2": {"row_count": 1, "execution_time_ms": 8}},
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s2"}],
                    "loop_iteration": 20,  # 必须等于或超过 MAX_ITERATIONS (20)
                },
            ),
        )
        exhausted_result = observer(exhausted_state)
        assert exhausted_result["loop_status"] == "failed"

    def test_quality_issue_detection_for_empty_results(self):
        """空结果应触发blocking质量问题并阻止下游执行（Oracle建议：失败隔离）"""
        observer = create_metric_observer()
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "generated_sql": "SELECT * FROM tmp_empty",
                    "step_results": {"s1": {"row_count": 0, "execution_time_ms": 22}},
                    "planner_observations": [],
                    "metric_plan_nodes": [{"step_id": "s1"}],
                    "loop_iteration": 0,
                },
            ),
        )

        result = observer(state)
        observations = cast(list[dict[str, object]], result["planner_observations"])
        observation = observations[0]

        assert observation["observation_type"] == "warning"
        quality_issues = cast(list[dict[str, object]], observation["quality_issues"])
        assert len(quality_issues) == 1
        assert quality_issues[0]["category"] == "empty_result"
        assert quality_issues[0]["severity"] == "blocking"
        # Oracle建议：空结果应阻止下游，step_status为failed_validation，loop_status为adjusting
        assert result["step_status"] == "failed_validation"
        status_map = cast(dict[str, str], result["step_status_map"])
        assert status_map["s1"] == "failed_validation"
        assert result["loop_status"] == "adjusting"
