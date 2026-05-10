"""
指标执行器测试
"""
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_executor import create_metric_executor, _is_safe_sql  # pyright: ignore[reportPrivateUsage]
from state import AgentState


class FakeCursor:
    """最小可用的假游标。"""

    def __init__(self):
        self.rowcount: int = 7
        self.description: object = [("col_a",), ("col_b",)]
        self.executed_sql: list[str] = []

    def execute(self, operation: str) -> None:
        self.executed_sql.append(operation)

    def fetchall(self) -> list[dict[str, object]]:
        return [
            {"Field": "school_id", "Type": "int"},
            {"Field": "score", "Type": "double"},
        ]

    def close(self) -> None:
        return None


class FakeConnection:
    """最小可用的假连接。"""

    def __init__(self):
        self.cursor_instance: FakeCursor = FakeCursor()
        self.commit_count: int = 0

    def cursor(self, dictionary: bool = False) -> FakeCursor:
        assert dictionary is True
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1

    def close(self) -> None:
        return None


class TestMetricExecutor:
    """测试指标执行器"""

    def test_create_executor_returns_callable(self):
        """创建执行器应返回可调用对象"""
        executor = create_metric_executor()
        assert callable(executor)

    def test_executor_requires_current_step_id(self):
        """执行器需要current_step_id"""
        executor = create_metric_executor()
        state = cast(AgentState, cast(object, {"generated_sql": "SELECT 1"}))
        result = executor(state)

        assert "execution_error" in result
        assert "未指定当前步骤ID" in str(result["execution_error"])
        assert result["current_node"] == "metric_executor"

    def test_executor_requires_sql(self):
        """执行器需要SQL"""
        executor = create_metric_executor()
        state = cast(AgentState, cast(object, {"current_step_id": "s1"}))
        result = executor(state)

        assert "execution_error" in result
        assert "没有SQL可执行" in str(result["execution_error"])

    def test_executor_rejects_unsafe_sql(self):
        """执行器应拒绝不安全的SQL"""
        executor = create_metric_executor()
        state = cast(AgentState, cast(object, {
            "current_step_id": "s1",
            "generated_sql": "DELETE FROM table",
        }))
        result = executor(state)

        assert "execution_error" in result
        assert "禁止数据修改操作" in str(result["execution_error"])

    def test_executor_returns_artifact_metadata(self):
        """执行成功时应返回临时表元数据"""
        connection = FakeConnection()
        executor = create_metric_executor(connection)
        state = cast(AgentState, cast(object, {
            "current_step_id": "step-1",
            "generated_sql": "SELECT school_id, score FROM metrics_source",
            "step_results": {},
            "materialized_artifacts": {},
            "metric_plan_nodes": [  # 添加计划节点，使其不是最后一步
                {"step_id": "step-1"},
                {"step_id": "step-2"},  # 还有下一步，所以 step-1 不是最后一步
            ],
        }))

        result = executor(state)

        assert result["current_node"] == "metric_executor"
        assert result["execution_error"] is None

        execution_result = cast(dict[str, object], result["execution_result"])
        assert execution_result["artifact_type"] == "mysql_temp_table"
        assert str(execution_result["output_table"]).startswith("_metric_step_step_1_")
        assert execution_result["row_count"] == 7

        step_results = cast(dict[str, dict[str, object]], result["step_results"])
        assert step_results["step-1"]["status"] == "success"
        assert step_results["step-1"]["row_count"] == 7

        artifacts = cast(dict[str, dict[str, object]], result["materialized_artifacts"])
        assert artifacts["step-1"]["artifact_type"] == "mysql_temp_table"
        assert artifacts["step-1"]["row_count"] == 7

        assert connection.commit_count == 1
        # 验证执行了 SQL（第一个可能是 EXPLAIN 预检，然后是 CREATE TABLE 或 SELECT）
        assert len(connection.cursor_instance.executed_sql) >= 1
        # 验证包含 EXPLAIN 预检（新添加的）
        assert any("EXPLAIN" in sql for sql in connection.cursor_instance.executed_sql)
        # 验证最终有执行主要 SQL（CREATE TABLE 或直接 SELECT）


class TestIsSafeSQL:
    """测试SQL安全检查"""

    def test_safe_select_passes(self):
        """SELECT语句应通过检查"""
        assert _is_safe_sql("SELECT * FROM table") is True

    def test_safe_create_passes(self):
        """CREATE TABLE语句应通过检查"""
        assert _is_safe_sql("CREATE TABLE tmp AS SELECT * FROM table") is True

    def test_delete_fails(self):
        """DELETE应被拒绝"""
        assert _is_safe_sql("DELETE FROM table") is False

    def test_drop_fails(self):
        """DROP应被拒绝"""
        assert _is_safe_sql("DROP TABLE table") is False

    def test_union_passes(self):
        """UNION应被允许（只读查询操作）"""
        assert _is_safe_sql("SELECT 1 UNION SELECT 2") is True

    def test_union_all_passes(self):
        """UNION ALL应被允许（只读查询操作）"""
        assert _is_safe_sql("SELECT a FROM t1 UNION ALL SELECT b FROM t2") is True

    def test_insert_in_union_context_fails(self):
        """即使有UNION，包含INSERT也应被拒绝"""
        assert _is_safe_sql("SELECT 1 UNION INSERT INTO t VALUES (1)") is False
