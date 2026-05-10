"""
指标SQL生成器测试
"""
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_sql_generator import (
    create_metric_sql_generator,
    _clean_sql,
    _build_normal_prompt,  # pyright: ignore[reportPrivateUsage]
    _build_retry_prompt,  # pyright: ignore[reportPrivateUsage]
)
from state import AgentState


class TestMetricSQLGenerator:
    """测试指标SQL生成器"""
    
    def test_create_generator_returns_callable(self):
        """创建生成器应返回可调用对象"""
        generator = create_metric_sql_generator()
        assert callable(generator)
    
    def test_generator_requires_current_step_id(self):
        """生成器需要current_step_id"""
        generator = create_metric_sql_generator()
        state = cast(AgentState, cast(object, {}))  # 缺少current_step_id
        result = generator(state)
        error_msg = str(result.get("execution_error", ""))
        
        assert "execution_error" in result
        assert "未指定当前步骤ID" in error_msg
        assert result["current_node"] == "metric_sql_generator"
    
    def test_generator_requires_plan_node(self):
        """生成器需要对应的计划节点"""
        generator = create_metric_sql_generator()
        state = cast(AgentState, cast(object, {
            "current_step_id": "s1",
            "metric_plan_nodes": []  # 空列表，找不到s1
        }))
        result = generator(state)
        error_msg = str(result.get("execution_error", ""))
        
        assert "execution_error" in result
        assert "未找到步骤ID对应的计划节点" in error_msg

    def test_generator_rejects_non_sql_model_output(self):
        """模型返回解释文本时，生成器应拒绝并返回 execution_error。"""

        class FakeModel:
            def invoke(self, prompt: str) -> str:
                _ = prompt
                return "根据你的问题，建议先检查 schema，再生成 SQL。"

        generator = create_metric_sql_generator(FakeModel())
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s1",
                            "intent_type": "filter",
                            "description": "筛选",
                            "required_tables": ["schools"],
                            "depends_on": [],
                        }
                    ],
                    "step_results": {},
                    "execution_history": [],
                },
            ),
        )
        result = generator(state)

        assert result.get("generated_sql") == ""
        assert "execution_error" in result
        assert "不是可执行SQL" in str(result.get("execution_error", ""))

    def test_retry_prompt_includes_planner_feedback(self):
        """重试时应把 planner 的失败反馈传给 SQL 生成器。"""

        class CaptureModel:
            def __init__(self) -> None:
                self.last_prompt = ""

            def invoke(self, prompt: str) -> str:
                self.last_prompt = prompt
                return "SELECT 1 AS ok;"

        model = CaptureModel()
        generator = create_metric_sql_generator(model)

        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s1",
                            "intent_type": "filter",
                            "description": "筛选",
                            "required_tables": ["schools"],
                            "depends_on": [],
                        }
                    ],
                    "schema_context": "tables...",
                    "step_results": {},
                    "execution_history": [
                        {"step_id": "s1", "status": "failed", "error": "Unknown column school_name"}
                    ],
                    "loop_decision": {"decision": "adjust", "reason": "上一步字段名错误，请修正"},
                    "planner_observations": [
                        {
                            "step_id": "s1",
                            "error_category": "schema_mismatch",
                            "error_summary": "列 school_name 不存在",
                            "sql_executed": "CREATE TABLE t AS SELECT school_name FROM schools;",
                        }
                    ],
                },
            ),
        )

        result = generator(state)

        generated_sql = str(result.get("generated_sql", ""))
        assert generated_sql.startswith("SELECT")
        assert "规划器反馈（必须参考）" in model.last_prompt
        assert "planner_decision: adjust" in model.last_prompt
        assert "planner_reason: 上一步字段名错误，请修正" in model.last_prompt
        assert "planner_error_summary: 列 school_name 不存在" in model.last_prompt

    def test_retry_prompt_includes_executor_failed_error_context(self):
        """即使 planner_observations 缺失，也应带上 executor 的失败上下文。"""

        class CaptureModel:
            def __init__(self) -> None:
                self.last_prompt = ""

            def invoke(self, prompt: str) -> str:
                self.last_prompt = prompt
                return "SELECT 1 AS ok;"

        model = CaptureModel()
        generator = create_metric_sql_generator(model)

        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s1",
                            "intent_type": "filter",
                            "description": "筛选",
                            "required_tables": ["schools"],
                            "depends_on": [],
                        }
                    ],
                    "schema_context": "tables...",
                    "step_results": {},
                    "execution_history": [
                        {
                            "step_id": "s1",
                            "status": "failed",
                            "sql": "CREATE TABLE t AS SELECT school_name FROM schools;",
                            "error": "Unknown column school_name",
                        }
                    ],
                    "loop_decision": {"decision": "adjust", "reason": "重试该步骤"},
                    "planner_observations": [],
                },
            ),
        )

        result = generator(state)

        generated_sql = str(result.get("generated_sql", ""))
        assert generated_sql.startswith("SELECT")
        assert "executor_error: Unknown column school_name" in model.last_prompt
        assert "executor_failed_sql: CREATE TABLE t AS SELECT school_name FROM schools;" in model.last_prompt

    def test_successful_generation_clears_stale_execution_error(self):
        """成功生成 SQL 时必须清空上一次残留 execution_error，避免错误路由。"""

        class FakeModel:
            def invoke(self, prompt: str) -> str:
                _ = prompt
                return "<SQL>SELECT school_id FROM schools;</SQL>"

        generator = create_metric_sql_generator(FakeModel())
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s1",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s1",
                            "intent_type": "filter",
                            "description": "筛选",
                            "required_tables": ["schools"],
                            "depends_on": [],
                        }
                    ],
                    "step_results": {},
                    "execution_history": [],
                    "execution_error": "上一轮失败残留错误",
                },
            ),
        )

        result = generator(state)

        assert result.get("generated_sql") == "SELECT school_id FROM schools;"
        assert result.get("execution_error") is None

    def test_retry_prompt_includes_intermediate_table_schema(self):
        """重试提示词必须包含上游临时表结构，避免错误 schema 污染。"""
        node = cast(
            dict[str, object],
            {
                "step_id": "s3",
                "intent_type": "derive",
                "description": "计算得分",
                "required_tables": ["s2_output"],
                "depends_on": ["s2"],
                "expected_outputs": ["school_id", "score"],
                "expected_grain": ["school_id"],
            },
        )
        step_results = {
            "s2": {
                "output_table": "_metric_step_s2_abcd",
                "columns": [
                    {"Field": "school_id", "Type": "int"},
                    {"Field": "score", "Type": "decimal(10,4)"},
                ],
            }
        }
        failed_attempts = cast(list[dict[str, object]], [{"error": "Unknown column level3_score"}])

        prompt = _build_retry_prompt(
            node=node,
            schema_context="tables...",
            step_results=cast(dict[str, dict[str, object]], step_results),
            materialized_schemas={},  # 新增参数
            failed_attempts=failed_attempts,
        )

        assert "上游临时表结构" in prompt
        assert "_metric_step_s2_abcd" in prompt
        assert "school_id: int" in prompt
        assert "score: decimal(10,4)" in prompt
        assert "预期输出字段" in prompt
        assert "school_id, score" in prompt
        assert "绝对不要从历史错误文本中推断新字段名" in prompt

    def test_non_first_step_prompt_deprioritizes_full_schema(self):
        """非首步应提示优先使用上游中间表结构。"""
        node = cast(
            dict[str, object],
            {
                "step_id": "s3",
                "intent_type": "derive",
                "description": "计算得分",
                "required_tables": ["s2_output"],
                "depends_on": ["s2"],
                "expected_outputs": ["school_id", "score"],
                "expected_grain": ["school_id"],
            },
        )
        step_results = {
            "s2": {
                "output_table": "_metric_step_s2_abcd",
                "columns": [{"Field": "school_id", "Type": "int"}],
            }
        }

        prompt = _build_normal_prompt(
            node=node,
            schema_context="very large schema...",
            step_results=cast(dict[str, dict[str, object]], step_results),
            materialized_schemas={},  # 新增参数
        )

        assert "当前步骤已有上游中间表输入，请优先使用上游临时表结构" in prompt

    def test_normal_prompt_includes_filters_and_like_guidance(self):
        """普通提示词应包含 plan.filters 和 LIKE 策略指导。"""
        node = cast(
            dict[str, object],
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选省份",
                "required_tables": ["schools"],
                "depends_on": [],
                "filters": [
                    {"field": "province", "operator": "like", "value": "北京"},
                    {"field": "year", "operator": "=", "value": 2023},
                ],
            },
        )

        prompt = _build_normal_prompt(
            node=node,
            schema_context="tables...",
            step_results=cast(dict[str, dict[str, object]], {}),
            materialized_schemas={},  # 新增参数
        )

        assert "计划中的筛选条件" in prompt
        assert "field=province, operator=like, value=北京" in prompt
        assert "field=year, operator==, value=2023" in prompt
        assert "文本字段筛选默认优先使用 LIKE" in prompt
        assert "数值/日期字段使用" in prompt


class TestCleanSQL:
    """测试SQL清理函数"""
    
    def test_clean_sql_removes_markdown(self):
        """清理应移除markdown代码块"""
        sql = "```sql\nSELECT * FROM table\n```"
        cleaned = _clean_sql(sql)
        assert "```" not in cleaned
        assert "SELECT * FROM table" in cleaned
    
    def test_clean_sql_removes_empty_lines(self):
        """清理应移除空行"""
        sql = "SELECT *\n\n\nFROM table"
        cleaned = _clean_sql(sql)
        lines = cleaned.split('\n')
        assert all(line.strip() for line in lines)

    def test_clean_sql_extracts_sql_from_explanatory_text(self):
        """清理应从解释文本中提取第一个可执行 SQL。"""
        sql = "先分析一下：\nCREATE TABLE t1 AS SELECT * FROM schools;\n以上是SQL"
        cleaned = _clean_sql(sql)
        assert cleaned.startswith("CREATE TABLE")
        assert cleaned.endswith(";")

    def test_clean_sql_trims_trailing_natural_language_without_semicolon(self):
        """无分号场景也应截断尾部解释文本。"""
        sql = "SELECT school_id, score FROM schools\n以上是SQL解释，请执行"
        cleaned = _clean_sql(sql)
        assert cleaned == "SELECT school_id, score FROM schools"

    def test_clean_sql_keeps_multiline_sql_and_drops_explanation_tail(self):
        """保留多行 SQL 主体，并剔除后续说明。"""
        sql = """SELECT school_id, score
FROM schools
WHERE province LIKE '%北京%'
ORDER BY score DESC
说明：上面是最终SQL"""
        cleaned = _clean_sql(sql)
        assert "说明" not in cleaned
        assert cleaned.startswith("SELECT school_id, score")
        assert "ORDER BY score DESC" in cleaned

    def test_clean_sql_extracts_sql_from_xml_tag(self):
        """应优先提取 <SQL> 标签中的 SQL 主体。"""
        sql = "思路略。<SQL>SELECT school_id FROM schools WHERE year = 2023;</SQL>额外说明"
        cleaned = _clean_sql(sql)
        assert cleaned == "SELECT school_id FROM schools WHERE year = 2023;"

    def test_prompt_requires_sql_tag_output(self):
        """提示词应显式要求 <SQL> 输出格式，减少冗余文本。"""
        node = cast(
            dict[str, object],
            {
                "step_id": "s1",
                "intent_type": "filter",
                "description": "筛选",
                "required_tables": ["schools"],
                "depends_on": [],
            },
        )
        prompt = _build_normal_prompt(
            node=node,
            schema_context="tables...",
            step_results=cast(dict[str, dict[str, object]], {}),
            materialized_schemas={},  # 新增参数
        )
        assert "<SQL>...SQL语句...</SQL>" in prompt
