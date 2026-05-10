"""SQL corrector 解析与 SQL-only 约束测试"""
import sys
from pathlib import Path
from typing import cast

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.sql_corrector import (
    create_sql_corrector,
    _extract_reflection_and_sql,  # pyright: ignore[reportPrivateUsage]
)
from prompts.sql_rules import DatabaseType
from state import AgentState


class _FakeLLM:
    def __init__(self, content: str) -> None:
        self.content = content

    def invoke(self, prompt: str):
        _ = prompt

        class _Resp:
            def __init__(self, content: str) -> None:
                self.content = content

        return _Resp(self.content)


def test_extract_reflection_and_sql_from_json_payload() -> None:
    text = '{"reflection":"字段名错误","sql":"SELECT `学校名称` FROM `schools` LIMIT 5;"}'
    reflection, sql = _extract_reflection_and_sql(text)
    assert "字段名错误" in reflection
    assert sql.startswith("SELECT")


def test_extract_reflection_and_sql_from_sql_tag() -> None:
    text = "说明如下 <SQL>WITH t AS (SELECT 1) SELECT * FROM t;</SQL>"
    reflection, sql = _extract_reflection_and_sql(text)
    assert sql.startswith("WITH")
    assert "SELECT * FROM t;" in sql
    assert reflection


def test_sql_corrector_rejects_non_sql_response() -> None:
    llm = _FakeLLM("这里是分析，不给SQL")
    corrector = create_sql_corrector(llm, database_type=DatabaseType.MYSQL)
    state = cast(
        AgentState,
        cast(
            object,
            {
                "user_query": "查询学校数量",
                "generated_sql": "SELECT * FROM schools",
                "execution_observation": "Unknown column",
                "assembled_prompt": "### 数据库 Schema\n{}",
                "correction_count": 0,
            },
        ),
    )
    result = corrector(state)
    assert result.get("generated_sql") == ""
    assert "未提取到可执行SQL" in str(result.get("execution_error", ""))
