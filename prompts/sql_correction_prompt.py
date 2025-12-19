"""
SQL 纠错提示词

参考 WrenAI 的 SQL Correction 设计，用于在 SQL 执行失败后生成纠正的 SQL。
"""

from .sql_rules import get_sql_rules, get_sql_correction_rules, DatabaseType


def get_sql_correction_system_prompt(database_type: DatabaseType = DatabaseType.MYSQL) -> str:
    """
    获取 SQL 纠错/反思的系统提示词 (ReAct 版)
    """
    sql_rules = get_sql_rules(database_type)
    correction_rules = get_sql_correction_rules()
    
    return f"""你是一个具备深度反思能力的数据分析专家。你的任务是分析 SQL 的执行观测结果（Observation），判断其是否符合用户意图，并在必要时进行修正。

### TASK ###
根据用户问题、之前生成的 SQL 和执行观测（Observation），你需要：
1. **反思分析 (Reflection)**: 
   - 如果 Observation 是错误消息，分析语法错误原因。
   - 如果 Observation 是空结果(0 rows)，分析是否筛选条件太严或枚举值不匹配（如库里是“海淀区”而你写了“海淀”）。
   - 如果 Observation 数据量过大，思考是否漏掉了聚合逻辑。
2. **生成修正 SQL**: 生成能更好解决用户问题的语法正确的 SQL。

### SQL CORRECTION INSTRUCTIONS ###
{correction_rules}

### SQL RULES ###
{sql_rules}

### FINAL ANSWER FORMAT ###
请严格按 JSON 格式输出，不要包含其他文字：
{{
    "reflection": "你对执行结果的分析和改进思路",
    "sql": "修正后的 SQL 语句"
}}
"""


# SQL 纠错用户提示词模板
SQL_CORRECTION_USER_PROMPT_TEMPLATE = """### DATABASE SCHEMA ###
{schema}

{metric_context}

{instructions}

### QUESTION ###
**用户原始问题:** {user_query}

**之前生成且执行过的 SQL:**
```sql
{invalid_sql}
```

**执行观测结果 (Observation):**
```
{observation}
```

请仔细分析以上观测结果，如果结果不符合预期（如查询为空或报错），请进行反思并给出纠正后的 SQL。
"""



def build_sql_correction_prompt(
    user_query: str,
    invalid_sql: str,
    observation: str,
    schema: str,
    metric_context: str = "",
    instructions: str = "",
) -> str:
    """
    构建 SQL 纠错/反思提示词
    """
    # 格式化可选部分
    _metric_context = f"### 指标上下文 ###\n{metric_context}\n" if metric_context else ""
    _instructions = f"### USER INSTRUCTIONS ###\n{instructions}\n" if instructions else ""
    
    return SQL_CORRECTION_USER_PROMPT_TEMPLATE.format(
        user_query=user_query,
        schema=schema,
        metric_context=_metric_context,
        instructions=_instructions,
        invalid_sql=invalid_sql,
        observation=observation,
    )

