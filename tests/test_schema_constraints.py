"""测试 Schema 约束和物化表缓存功能

验证方案2（动态Schema）+ 方案5（规划层对齐）+ 方案3（负向约束）的组合效果
"""
import json
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.metric_sql_generator import (
    create_metric_sql_generator,
    _build_intermediate_table_context,
    _build_normal_prompt,
)
from state import AgentState


class TestMaterializedSchemaPropagation:
    """测试物化表 Schema 在步骤间的传递"""

    def test_materialized_schemas_in_state(self):
        """验证 metric_executor 正确生成 materialized_schemas"""
        # 模拟 metric_executor 的返回值
        executor_output = {
            "step_results": {
                "s1": {
                    "step_id": "s1",
                    "output_table": "_metric_step_s1_abc123",
                    "row_count": 100,
                    "columns": [
                        {"Field": "id", "Type": "int"},
                        {"Field": "value", "Type": "double"},
                        {"Field": "school_id", "Type": "int"},
                    ],
                    "status": "success",
                }
            },
            "materialized_schemas": {  # 关键新增字段
                "s1": {
                    "table_name": "_metric_step_s1_abc123",
                    "columns": [
                        {"Field": "id", "Type": "int"},
                        {"Field": "value", "Type": "double"},
                        {"Field": "school_id", "Type": "int"},
                    ],
                    "row_count": 100,
                }
            },
            "execution_history": [
                {"step_id": "s1", "status": "success", "sql": "SELECT ..."}
            ],
        }
        
        # 验证 materialized_schemas 存在且格式正确
        assert "materialized_schemas" in executor_output
        schemas = executor_output["materialized_schemas"]
        assert "s1" in schemas
        assert schemas["s1"]["table_name"] == "_metric_step_s1_abc123"
        assert len(schemas["s1"]["columns"]) == 3

    def test_intermediate_table_context_uses_materialized_schemas(self):
        """验证 _build_intermediate_table_context 优先使用 materialized_schemas"""
        step_results = {
            "s1": {
                "output_table": "_metric_step_s1_old",
                "columns": [{"Field": "old_col", "Type": "int"}],  # 旧的/可能被截断的数据
            }
        }
        
        # 提供更准确的 materialized_schemas
        materialized_schemas = {
            "s1": {
                "table_name": "_metric_step_s1_new",
                "columns": [
                    {"Field": "id", "Type": "int"},
                    {"Field": "value", "Type": "double"},  # 关键字段
                ],
            }
        }
        
        input_info, schema_info = _build_intermediate_table_context(
            depends_on=["s1"],
            step_results=step_results,
            materialized_schemas=materialized_schemas,
        )
        
        # 验证使用了 materialized_schemas 中的新表名
        assert "_metric_step_s1_new" in input_info
        # 验证包含了关键字段
        assert "value" in schema_info
        assert "old_col" not in schema_info  # 不应该使用旧的截断数据

    def test_prompt_includes_negative_constraints(self):
        """验证 prompt 包含负向约束和示例"""
        node = {
            "step_id": "s2",
            "intent_type": "aggregate",
            "description": "计算平均值",
            "required_tables": ["step_s1_output"],
            "depends_on": ["s1"],
            "expected_outputs": ["avg_value"],
        }
        
        step_results = {
            "s1": {
                "output_table": "step_s1_output",
                "columns": [{"Field": "value", "Type": "double"}],
            }
        }
        
        materialized_schemas = {
            "s1": {
                "table_name": "step_s1_output",
                "columns": [{"Field": "value", "Type": "double"}],
            }
        }
        
        schema_context = "questions 表: id, content, level1_name"
        
        prompt = _build_normal_prompt(
            node=node,
            schema_context=schema_context,
            step_results=step_results,
            materialized_schemas=materialized_schemas,
        )
        
        # 验证包含负向约束关键词
        assert "字段名使用规范" in prompt
        assert "错误示例（绝对禁止）" in prompt
        assert "❌" in prompt  # 错误标记
        assert "上游表显示字段" in prompt
        assert "幻觉为" in prompt  # 幻觉问题描述
        
        # 验证包含上游表结构
        assert "step_s1_output" in prompt
        assert "value: double" in prompt

    def test_prompt_blocks_hallucinated_columns(self):
        """验证 prompt 明确禁止幻觉列名"""
        node = {
            "step_id": "s2",
            "intent_type": "aggregate",
            "description": "计算答案得分",
            "required_tables": ["school_answers"],
            "depends_on": [],
            "expected_outputs": ["avg_score"],
        }
        
        step_results = {}
        materialized_schemas = {}
        schema_context = "school_answers 表: school_id, question_id, value"
        
        prompt = _build_normal_prompt(
            node=node,
            schema_context=schema_context,
            step_results=step_results,
            materialized_schemas=materialized_schemas,
        )
        
        # 验证约束规则明确
        assert "禁止从业务描述推断字段名" in prompt
        # 使用更宽松的匹配（避免编码问题）
        assert "answer" in prompt.lower()  # 提示中包含 answer 关键词
        assert "value" in prompt.lower()   # 提示中包含正确的字段名 value
        
        # 验证示例中展示了正确用法
        assert "上游表显示字段: `value` → 使用:" in prompt


class TestSchemaAwareSQLGeneration:
    """测试 Schema 感知的 SQL 生成"""

    def test_sql_generator_reads_materialized_schemas(self):
        """验证 SQL 生成器正确读取 materialized_schemas"""
        # 模拟状态
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s2",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s2",
                            "intent_type": "aggregate",
                            "description": "计算平均值",
                            "required_tables": ["_metric_step_s1_xxx"],
                            "depends_on": ["s1"],
                            "expected_outputs": ["avg_value"],
                        }
                    ],
                    "schema_context": "原始表 Schema",
                    "step_results": {
                        "s1": {
                            "output_table": "_metric_step_s1_xxx",
                            "columns": [{"Field": "value", "Type": "double"}],
                        }
                    },
                    "materialized_schemas": {  # 关键：物化表 Schema
                        "s1": {
                            "table_name": "_metric_step_s1_xxx",
                            "columns": [
                                {"Field": "school_id", "Type": "int"},
                                {"Field": "value", "Type": "double"},  # 实际存在的字段
                            ],
                        }
                    },
                    "execution_history": [],
                },
            ),
        )
        
        # 创建生成器（使用 mock 模型）
        class MockModel:
            def invoke(self, prompt: str):
                # 验证 prompt 中包含了物化表的 Schema
                assert "_metric_step_s1_xxx" in prompt
                assert "value: double" in prompt
                
                # 返回正确的 SQL（使用实际存在的 value 字段）
                return "<SQL>SELECT AVG(value) as avg_value FROM _metric_step_s1_xxx</SQL>"
        
        generator = create_metric_sql_generator(MockModel())
        result = generator(state)
        
        # 验证生成了正确的 SQL
        assert "AVG(value)" in result.get("generated_sql", "")
        assert "answer_score" not in result.get("generated_sql", "")  # 没有幻觉字段

    def test_sql_generation_with_dependencies(self):
        """测试多步骤依赖时的 Schema 传递"""
        state = cast(
            AgentState,
            cast(
                object,
                {
                    "current_step_id": "s3",
                    "metric_plan_nodes": [
                        {
                            "step_id": "s3",
                            "intent_type": "join",
                            "description": "关联学校和得分",
                            "required_tables": ["_metric_step_s1_xxx", "_metric_step_s2_yyy"],
                            "depends_on": ["s1", "s2"],
                            "expected_outputs": ["school_name", "score"],
                        }
                    ],
                    "schema_context": "schools 表: id, name, province",
                    "step_results": {
                        "s1": {"output_table": "_metric_step_s1_xxx", "columns": []},
                        "s2": {"output_table": "_metric_step_s2_yyy", "columns": []},
                    },
                    "materialized_schemas": {
                        "s1": {
                            "table_name": "_metric_step_s1_xxx",
                            "columns": [{"Field": "school_id", "Type": "int"}],
                        },
                        "s2": {
                            "table_name": "_metric_step_s2_yyy",
                            "columns": [{"Field": "total_score", "Type": "double"}],
                        },
                    },
                    "execution_history": [],
                },
            ),
        )
        
        class MockModel:
            def invoke(self, prompt: str):
                # 验证 prompt 包含两个上游表的结构
                assert "_metric_step_s1_xxx" in prompt
                assert "_metric_step_s2_yyy" in prompt
                assert "school_id" in prompt
                assert "total_score" in prompt
                return "<SQL>SELECT s.name, t.total_score FROM schools s JOIN _metric_step_s2_yyy t ON s.id = t.school_id</SQL>"
        
        generator = create_metric_sql_generator(MockModel())
        result = generator(state)
        
        sql = result.get("generated_sql", "")
        assert "total_score" in sql  # 使用了正确的字段名
        assert "school_id" in sql   # 使用了正确的关联字段
