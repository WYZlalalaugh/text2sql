"""SQL generator 清洗与校验测试"""
import sys
from pathlib import Path

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from agents.sql_generator import clean_sql, looks_like_sql


class TestSqlGeneratorCleaning:
    def test_clean_sql_extracts_from_sql_tag(self):
        raw = "解释略。<SQL>SELECT `学校名称` FROM `schools` LIMIT 10;</SQL>后续说明"
        cleaned = clean_sql(raw)
        assert cleaned == "SELECT `学校名称` FROM `schools` LIMIT 10;"

    def test_clean_sql_trims_explanatory_tail_without_semicolon(self):
        raw = "SELECT `学校名称`\nFROM `schools`\n解释：这是查询"
        cleaned = clean_sql(raw)
        assert cleaned == "SELECT `学校名称`\nFROM `schools`"

    def test_looks_like_sql_accepts_select_with_create(self):
        assert looks_like_sql("SELECT 1;")
        assert looks_like_sql("WITH t AS (SELECT 1) SELECT * FROM t;")
        assert looks_like_sql("CREATE TABLE t AS SELECT 1;")

    def test_looks_like_sql_rejects_explanation_only(self):
        assert not looks_like_sql("这是SQL：请执行")
        assert not looks_like_sql("```sql\n-- 说明\n```")
