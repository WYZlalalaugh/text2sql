"""
SQL 纠错提示词

参考 WrenAI 的 SQL Correction 设计，用于在 SQL 执行失败后生成纠正的 SQL。
"""

from .sql_rules import get_sql_rules, get_sql_correction_rules, DatabaseType


def get_sql_correction_system_prompt(database_type: DatabaseType = DatabaseType.MYSQL) -> str:
    """
    获取 SQL 纠错的系统提示词
    
    Args:
        database_type: 数据库类型
        
    Returns:
        系统提示词
    """
    sql_rules = get_sql_rules(database_type)
    correction_rules = get_sql_correction_rules()
    
    return f"""你是一个 ANSI SQL 专家，拥有杰出的逻辑思维和调试技能，你需要修复语法错误的 SQL 查询。

### TASK ###
给定一个执行失败的 SQL 查询和错误消息，你需要：
1. 深入分析错误消息，找到根本原因
2. 生成语法正确的 SQL 查询来修正错误

### SQL CORRECTION INSTRUCTIONS ###
{correction_rules}

### SQL RULES ###
确保你严格遵循以下 SQL 规则：

{sql_rules}

### FINAL ANSWER FORMAT ###
最终答案必须是 JSON 格式：

{{
    "sql": <CORRECTED_SQL_QUERY_STRING>
}}
"""


# SQL 纠错用户提示词模板
SQL_CORRECTION_USER_PROMPT_TEMPLATE = """### DATABASE SCHEMA ###
{schema}

{metric_context}

{sql_functions}

{instructions}

### QUESTION ###
**原始错误的 SQL:**
```sql
{invalid_sql}
```

**错误消息:**
```
{error_message}
```

请分析错误原因并生成纠正后的 SQL。
"""


def build_sql_correction_prompt(
    invalid_sql: str,
    error_message: str,
    schema: str,
    metric_context: str = "",
    sql_functions: str = "",
    instructions: str = "",
) -> str:
    """
    构建 SQL 纠错提示词
    
    Args:
        invalid_sql: 错误的 SQL
        error_message: 数据库返回的错误消息
        schema: 数据库 Schema
        metric_context: 指标上下文（可选）
        sql_functions: SQL 函数说明（可选）
        instructions: 用户指令（可选）
        
    Returns:
        完整的用户提示词
    """
    # 格式化可选部分
    _metric_context = f"### 指标上下文 ###\n{metric_context}\n" if metric_context else ""
    _sql_functions = f"### SQL FUNCTIONS ###\n{sql_functions}\n" if sql_functions else ""
    _instructions = f"### USER INSTRUCTIONS ###\n{instructions}\n" if instructions else ""
    
    return SQL_CORRECTION_USER_PROMPT_TEMPLATE.format(
        schema=schema,
        metric_context=_metric_context,
        sql_functions=_sql_functions,
        instructions=_instructions,
        invalid_sql=invalid_sql,
        error_message=error_message,
    )
