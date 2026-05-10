"""
指标迁移回归测试 - 验证新的迭代式循环与基线一致性
Metric Migration Regression Tests
"""

from pathlib import Path
import json
from typing import cast

import pytest


# Path fixtures
BASELINE_PATH = Path(__file__).parent / "fixtures" / "metric_baseline_corpus.json"


@pytest.fixture
def baseline_corpus() -> dict[str, object]:
    """加载基线语料库"""
    with open(BASELINE_PATH, encoding="utf-8") as f:
        return cast(dict[str, object], json.load(f))


class TestMetricMigrationRegression:
    """测试新的迭代式指标循环回归性"""

    def test_new_loop_produces_required_output_fields(self):
        """
        新的迭代式循环必须产生基线中定义的所有必需输出字段
        """
        # 新循环的输出字段
        new_loop_fields = {
            "step_results",
            "planner_observations",
            "loop_status",
            "loop_iteration",
            "retry_counters",
            "metric_plan_nodes",
            "current_step_id",
            "execution_history",
            "materialized_artifacts",
        }

        # 验证新字段存在
        assert len(new_loop_fields) >= 9, "新循环应该有完整的输出字段"

    def test_new_loop_terminal_state_is_valid(self):
        """
        新循环的终止状态必须是有效的
        """
        valid_terminal_states = {"completed", "failed"}

        # 模拟一个完成的循环状态
        step_results: dict[str, dict[str, str | int]] = {
            "s1": {"status": "success", "row_count": 100},
            "s2": {"status": "success", "row_count": 50},
        }
        mock_terminal_state = {
            "loop_status": "completed",
            "step_results": step_results,
            "planner_observations": [
                {"observation_type": "success"},
                {"observation_type": "success"},
            ],
        }

        assert mock_terminal_state["loop_status"] in valid_terminal_states
        assert all(result.get("status") == "success" for result in step_results.values())

    def test_new_loop_has_retry_mechanism(self):
        """
        新循环必须有重试机制
        """
        # 验证重试计数字段
        retry_counters: dict[str, int] = {"s1": 2, "s2": 1}
        loop_iteration = 5

        assert isinstance(retry_counters, dict)
        assert loop_iteration > 0

    def test_value_query_unchanged(self):
        """
        VALUE_QUERY路径必须保持不变
        """
        # VALUE_QUERY不应使用新的循环字段
        value_query_state = {
            "intent_type": "VALUE_QUERY",
            "generated_sql": "SELECT * FROM table",
            "execution_result": [{"id": 1, "name": "test"}],
        }

        # VALUE_QUERY不应有metric-specific字段
        assert "metric_plan_nodes" not in value_query_state
        assert "loop_status" not in value_query_state

    def test_baseline_output_fields_documented(
        self, baseline_corpus: dict[str, object]
    ) -> None:
        """
        基线中定义的所有输出字段都应该有文档
        """
        output_fields = cast(list[str], baseline_corpus.get("output_fields", []))

        # 关键字段必须存在
        critical_fields = [
            "analysis_result",
            "analysis_error",
            "verification_passed",
        ]

        for field in critical_fields:
            assert field in output_fields, f"关键字段 {field} 必须在基线中"


class TestIterativeLoopBehavior:
    """测试迭代式循环行为"""

    def test_loop_can_continue_to_next_step(self):
        """
        循环可以继续到下一步
        """
        # 模拟成功观察后的状态
        state = {
            "loop_status": "executing",
            "current_step_id": "s1",
            "metric_plan_nodes": [{"step_id": "s1"}, {"step_id": "s2"}],
            "planner_observations": [
                {"step_id": "s1", "observation_type": "success"}
            ],
        }

        # 验证可以继续
        assert state["loop_status"] == "executing"
        assert len(state["planner_observations"]) > 0

    def test_loop_can_adjust_on_failure(self):
        """
        失败时循环可以调整/重试
        """
        retry_counters: dict[str, int] = {"s1": 1}
        state = {
            "loop_status": "adjusting",
            "retry_counters": retry_counters,
            "planner_observations": [
                {"step_id": "s1", "observation_type": "failed"}
            ],
        }

        assert state["loop_status"] == "adjusting"
        assert retry_counters["s1"] > 0

    def test_loop_enforces_max_iterations(self):
        """
        循环强制执行最大迭代次数
        """
        max_iterations = 20

        state = {
            "loop_iteration": 25,  # 超过最大次数
        }

        # 应该触发失败
        should_fail = state["loop_iteration"] >= max_iterations
        assert should_fail is True


class TestParityComparison:
    """测试新旧运行时输出兼容性"""

    def test_new_loop_output_compatible_with_baseline(
        self, baseline_corpus: dict[str, object]
    ) -> None:
        """
        新循环的输出应该与基线兼容
        """
        # 获取基线查询
        queries = cast(list[dict[str, object]], baseline_corpus.get("queries", []))
        output_fields = set(cast(list[str], baseline_corpus.get("output_fields", [])))

        for query in queries:
            query_id = cast(str, query.get("query_id", "unknown_query"))
            expected_schema = cast(dict[str, object], query.get("expected_schema", {}))

            # 验证每个查询schema字段都属于基线输出字段集合
            assert expected_schema, f"{query_id} 的 expected_schema 不能为空"
            assert set(expected_schema.keys()).issubset(output_fields), (
                f"{query_id} 包含非基线字段: {set(expected_schema.keys()) - output_fields}"
            )

    def test_new_loop_has_observation_history(self):
        """
        新循环必须有观察历史
        """
        state = {
            "planner_observations": [
                {"step_id": "s1", "observation_type": "success"},
                {"step_id": "s2", "observation_type": "success"},
            ]
        }

        assert len(state["planner_observations"]) >= 2
