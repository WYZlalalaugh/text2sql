"""
指标SQL生成器 - 为迭代式指标循环生成SQL
Metric SQL Generator - generates SQL for iterative metric loop
"""
from __future__ import annotations

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false

from collections.abc import Mapping
import re
from typing import Protocol, TypeAlias, cast

from state import AgentState
from config import config

MetricPlanNode: TypeAlias = dict[str, object]
StepResult: TypeAlias = dict[str, object]


class _ModelClient(Protocol):
    def invoke(self, prompt: str) -> object:
        ...


def create_metric_sql_generator(model_client: _ModelClient | None = None):
    """
    创建指标SQL生成器节点
    
    Args:
        model_client: 可选的模型客户端，如果不提供则使用配置的LLM API
    """
    
    def metric_sql_generator_node(state: AgentState) -> dict[str, object]:
        """
        指标SQL生成节点 - 根据步骤意图和执行历史生成SQL
        
        输入状态:
        - current_step_id: 当前要执行的步骤ID
        - metric_plan_nodes: 计划节点列表
        - step_results: 之前步骤的结果
        - execution_history: 执行历史（用于重试）
        
        输出状态:
        - generated_sql: 生成的SQL
        - execution_error: 错误信息（如有）
        - current_node: 当前节点名称
        """
        current_step_id = state.get("current_step_id")
        plan_nodes = state.get("metric_plan_nodes") or []
        step_results = state.get("step_results") or {}
        execution_history = state.get("execution_history") or []
        planner_observations = state.get("planner_observations") or []
        loop_decision = state.get("loop_decision") or {}

        raw_schema_context = state.get("schema_context")
        schema_context = raw_schema_context if isinstance(raw_schema_context, str) else ""
        
        # 获取物化表的 Schema 缓存（关键改进）
        materialized_schemas = state.get("materialized_schemas") or {}
        
        if not current_step_id:
            return {
                "execution_error": "未指定当前步骤ID",
                "current_node": "metric_sql_generator"
            }
        
        # 找到当前步骤的意图
        current_node: MetricPlanNode | None = None
        for node in plan_nodes:
            if node.get("step_id") == current_step_id:
                current_node = node
                break
        
        if not current_node:
            return {
                "execution_error": f"未找到步骤ID对应的计划节点: {current_step_id}",
                "current_node": "metric_sql_generator"
            }
        
        # 检查执行历史（用于重试）
        node_history = [
            record for record in execution_history
            if record.get("step_id") == current_step_id
        ]
        failed_attempts = [
            record for record in node_history
            if record.get("status") == "failed"
        ]
        
        # 判断是否为最后一步
        is_final_step = _is_final_step_in_plan(current_step_id, plan_nodes)
        planner_feedback = _extract_planner_feedback(
            current_step_id=str(current_step_id),
            planner_observations=planner_observations,
            loop_decision=loop_decision,
            execution_history=execution_history,
        )
        
        try:
            # 构建提示词
            if not failed_attempts:
                # 首次尝试
                prompt = _build_normal_prompt(
                    current_node,
                    schema_context,
                    step_results,
                    materialized_schemas,  # 新增：物化表Schema
                    is_final_step,
                    planner_feedback,
                )
            else:
                # 重试 - 包含错误历史
                prompt = _build_retry_prompt(
                    current_node,
                    schema_context,
                    step_results,
                    materialized_schemas,  # 新增：物化表Schema
                    failed_attempts,
                    is_final_step,
                    planner_feedback,
                )
            
            # 生成SQL
            if model_client is not None:
                response = model_client.invoke(prompt)
                response_content = getattr(response, "content", None)
                sql = response_content if isinstance(response_content, str) else str(response)
            else:
                sql = _call_llm_api(prompt)
            
            # 清理SQL
            sql = _clean_sql(sql)

            if not _looks_like_sql(sql):
                return {
                    "generated_sql": "",
                    "execution_error": "SQL生成失败: 模型返回的内容不是可执行SQL",
                    "current_node": "metric_sql_generator",
                }

            if is_final_step and not sql.strip().upper().startswith("SELECT"):
                return {
                    "generated_sql": "",
                    "execution_error": "SQL生成失败: 最后一步必须生成 SELECT 语句",
                    "current_node": "metric_sql_generator",
                }
            
            return {
                "generated_sql": sql,
                # Clear stale error from previous attempts so graph routing can
                # correctly proceed to metric_executor on successful generation.
                "execution_error": None,
                "current_node": "metric_sql_generator"
            }
            
        except Exception as e:
            return {
                "generated_sql": "",
                "execution_error": f"SQL生成失败: {str(e)}",
                "current_node": "metric_sql_generator"
            }
    
    return metric_sql_generator_node


def _build_normal_prompt(
    node: MetricPlanNode,
    schema_context: str,
    step_results: Mapping[str, StepResult],
    materialized_schemas: Mapping[str, dict[str, object]],  # 新增
    is_final_step: bool = False,
    planner_feedback: str = "",
) -> str:
    """构建首次执行的提示词"""
    
    intent_type = node.get("intent_type", "unknown")
    description = node.get("description", "")
    required_tables = _as_str_list(node.get("required_tables"))
    depends_on = _as_str_list(node.get("depends_on"))
    expected_outputs = _as_str_list(node.get("expected_outputs"))
    expected_grain = _as_str_list(node.get("expected_grain"))
    step_filters = _coerce_filters(node.get("filters"))
    
    # 构建输入表与上游临时表结构信息
    input_tables_info, intermediate_table_schemas = _build_intermediate_table_context(
        depends_on=depends_on,
        step_results=step_results,
        materialized_schemas=materialized_schemas,  # 新增
    )
    schema_block = _format_schema_block(
        schema_context=schema_context,
        depends_on=depends_on,
        has_intermediate_schema=bool(intermediate_table_schemas),
    )
    
    # 根据是否为最后一步，生成不同的SQL要求
    if is_final_step:
        sql_requirement = """## SQL生成要求（这是最后一步！）
1. **只生成纯 SELECT 语句，不要生成 CREATE TABLE**
2. 这是查询的最后一步，结果将直接返回给用户
3. 确保包含所有需要输出给用户的字段
4. 确保SQL语法兼容MySQL 8.0"""
    else:
        sql_requirement = """## SQL生成要求（中间步骤）
1. 使用 CREATE TABLE 语句物化结果，例如: CREATE TABLE temp_table AS SELECT ...
2. 这是中间步骤，结果将供后续步骤使用
3. 确保SQL语法兼容MySQL 8.0"""
    
    prompt = f"""你是一个SQL生成专家。请根据以下步骤意图生成MySQL兼容的SQL语句。

## 步骤意图
- 步骤ID: {node.get('step_id')}
- 意图类型: {intent_type}
- 描述: {description}
- 是否为最后一步: {'是' if is_final_step else '否'}

## 输入表
{chr(10).join(f"- {t}" for t in required_tables) if required_tables else "（从数据库Schema获取）"}
{input_tables_info}

## 上游临时表结构（关键！）
这些表是之前步骤生成的中间结果，查询这些表时需要使用以下字段结构：
{intermediate_table_schemas if intermediate_table_schemas else "（无上游临时表）"}

## 计划中的筛选条件
{_format_filter_hint(step_filters)}

## 规划器反馈（若有）
{planner_feedback or "（无）"}

## 数据库Schema（原始表）
```
{schema_block}
```

## 输出要求
- 预期输出字段: {', '.join(expected_outputs) if expected_outputs else '根据意图推断'}
- 预期粒度: {', '.join(expected_grain) if expected_grain else '根据意图推断'}

    {sql_requirement}
4. 查询上游临时表时，必须使用"上游临时表结构"中定义的字段名
5. 文本字段筛选默认优先使用 LIKE（模糊匹配），除非用户明确要求精确匹配
6. 数值/日期字段使用 =、>、<、between、in 等精确/范围比较，不要滥用 LIKE
7. 若使用 LIKE，注意转义文本中的 `%` 和 `_`（必要时使用 ESCAPE）
8. **MySQL 8.0 临时表限制（关键！）**：同一查询中**禁止对同一个临时表引用两次**（含子查询），否则报 `Can't reopen table`
   - **错误示例1**：```sql SELECT * FROM t1 WHERE val = (SELECT MIN(val) FROM t1) ``` — t1被打开两次
   - **错误示例2**：```sql WITH t AS (...) SELECT * FROM t CROSS JOIN (SELECT * FROM t) ```
   - **正确做法**：用窗口函数或ORDER BY替代子查询引用同一临时表，如 `SELECT ..., RANK() OVER (ORDER BY val ASC) FROM t1`
9. **窗口函数规则（关键！）**：
   - 不能在 RANK()、ROW_NUMBER() 等窗口函数的 ORDER BY 子句中嵌套其他窗口函数（如 MIN() OVER、MAX() OVER）
   - **错误示例**：`RANK() OVER (ORDER BY (value - MIN(value) OVER ()) / (MAX(value) OVER () - MIN(value) OVER ()))`
   - **正确做法**：先用子查询或CTE计算归一化值，再在外面用 RANK()
10. **只输出MySQL 8.0兼容的SQL代码**，严禁生成其他数据库方言
11. 绝对不要输出"SQL:", "解释:", "思路:", markdown 代码块等非SQL内容
12. 输出格式必须为：<SQL>...SQL语句...</SQL>
13. **筛选值必须直接取自"计划中的筛选条件"的value字段（关键！）**：禁止从描述文本推断筛选值；若描述与filters矛盾，以filters为准

## 字段名使用规范（关键！）
**正确示例**：
- 上游表显示字段: `value` → 使用: `SELECT value FROM ...`
- 上游表显示字段: `school_id` → 使用: `SELECT school_id FROM ...`

**错误示例（绝对禁止）**：
- ❌ 上游表显示字段: `value` → 幻觉为: `answer_score`
- ❌ 上游表显示字段: `school_id` → 幻觉为: `id`
- ❌ 假设存在字段: `score`（实际Schema中不存在）

**约束规则**：
- 仅使用"上游临时表结构"和"数据库Schema"中明确列出的字段名
- 禁止从业务描述推断字段名（如"答案"不等于`answer`，可能是`value`）
- 禁止假设任何表有`id`主键字段，必须使用Schema中显示的实际主键名
- 如果业务术语在Schema中找不到对应字段，必须在SQL中注释说明使用了哪个实际字段

请生成SQL：
"""
    return prompt


def _build_retry_prompt(
    node: MetricPlanNode,
    schema_context: str,
    step_results: Mapping[str, StepResult],
    materialized_schemas: Mapping[str, dict[str, object]],  # 新增
    failed_attempts: list[dict[str, object]],
    is_final_step: bool = False,
    planner_feedback: str = "",
) -> str:
    """构建重试的提示词，包含错误历史"""
    
    intent_type = node.get("intent_type", "unknown")
    description = node.get("description", "")
    required_tables = _as_str_list(node.get("required_tables"))
    depends_on = _as_str_list(node.get("depends_on"))
    expected_outputs = _as_str_list(node.get("expected_outputs"))
    expected_grain = _as_str_list(node.get("expected_grain"))
    step_filters = _coerce_filters(node.get("filters"))

    input_tables_info, intermediate_table_schemas = _build_intermediate_table_context(
        depends_on=depends_on,
        step_results=step_results,
        materialized_schemas=materialized_schemas,  # 新增
    )
    schema_block = _format_schema_block(
        schema_context=schema_context,
        depends_on=depends_on,
        has_intermediate_schema=bool(intermediate_table_schemas),
    )
    
    # 构建错误历史（精简格式，突出MySQL错误）
    error_parts = []
    for i, record in enumerate(failed_attempts[-3:], 1):  # 只取最近3次
        sql = record.get('sql', 'N/A')
        error = record.get('error', 'N/A')
        # 提取MySQL错误代码和消息
        error_msg = str(error)
        if 'MySQL' not in error_msg and 'SQL' not in error_msg:
            error_msg = f"MySQL错误: {error_msg}"
        error_parts.append(f"尝试 {i}:\nSQL:\n{sql}\n\n错误:\n{error_msg}")
    
    error_history = "\n\n---\n\n".join(error_parts) if error_parts else "（无）"
    
    # 根据是否为最后一步，生成不同的SQL要求
    if is_final_step:
        sql_requirement = """## SQL生成要求（这是最后一步！）
1. **只生成纯 SELECT 语句，不要生成 CREATE TABLE**
2. 这是查询的最后一步，结果将直接返回给用户
3. 确保包含所有需要输出给用户的字段"""
    else:
        sql_requirement = """## SQL生成要求（中间步骤）
1. 使用 CREATE TABLE 语句物化结果，例如: CREATE TABLE temp_table AS SELECT ...
2. 这是中间步骤，结果将供后续步骤使用"""
    
    prompt = f"""你是一个MySQL 8.0 SQL生成专家。请分析以下错误并生成修正后的SQL。

## 步骤意图
- 步骤ID: {node.get('step_id')}
- 意图类型: {intent_type}
- 描述: {description}
- 是否为最后一步: {'是' if is_final_step else '否'}

## 输入表
{chr(10).join(f"- {t}" for t in required_tables) if required_tables else "（从数据库Schema获取）"}
{input_tables_info}

## 上游临时表结构（关键！）
这些表是之前步骤生成的中间结果，查询这些表时必须使用以下字段结构：
{intermediate_table_schemas if intermediate_table_schemas else "（无上游临时表）"}

## 计划中的筛选条件
{_format_filter_hint(step_filters)}

## 错误历史
{error_history}

## 规划器反馈（简化）
{planner_feedback or "（无）"}

## 数据库Schema
```
{schema_block}
```

## 输出要求
- 预期输出字段: {', '.join(expected_outputs) if expected_outputs else '根据意图推断'}
- 预期粒度: {', '.join(expected_grain) if expected_grain else '根据意图推断'}

    {sql_requirement}
3. **必须严格遵循MySQL 8.0语法**
4. 查询上游临时表时，必须使用"上游临时表结构"中定义的字段名
5. 绝对不要从历史错误文本中推断新字段名或新表结构
6. 仅以"上游临时表结构"和"数据库Schema"作为字段来源
7. 文本字段筛选默认优先使用 LIKE（模糊匹配），除非用户明确要求精确匹配
8. 数值/日期字段使用精确比较（=、between 等），避免 LIKE
9. **MySQL 8.0 CTE临时表限制（关键！）**：
   - **禁止**在同一查询中多次引用带别名的临时表（CTE），会导致"Can't reopen table"错误
   - **错误示例**：```sql WITH t AS (...) SELECT * FROM _metric_step_s3 t CROSS JOIN (SELECT * FROM _metric_step_s3) mm ```
   - **正确做法**：使用子查询代替CTE，或避免对同一临时表使用别名多次引用
10. **窗口函数规则（关键！）**：
    - 不能在 RANK()、ROW_NUMBER() 等窗口函数的 ORDER BY 子句中嵌套其他窗口函数
11. **只生成MySQL 8.0兼容的SQL代码**
12. 输出格式必须为：<SQL>...SQL语句...</SQL>
13. **筛选值必须直接取自"计划中的筛选条件"的value字段（关键！）**：禁止从描述文本推断筛选值；若描述与filters矛盾，以filters为准

## 字段名使用规范
- 仅使用"上游临时表结构"和"数据库Schema"中明确列出的字段名
- 禁止从业务描述推断字段名
- 禁止假设任何表有`id`主键字段

请生成修正后的SQL：
"""
    return prompt


def _fetch_table_schema_from_db(table_name: str) -> list[dict[str, object]]:
    """
    从 MySQL 实时查询表结构（第三层兜底方案）

    Args:
        table_name: 表名（可以是临时表）

    Returns:
        列信息列表，格式为 [{"Field": "col1", "Type": "int", ...}, ...]
        失败时返回空列表
    """
    import logging
    logger = logging.getLogger(__name__)

    try:
        import mysql.connector

        # 验证数据库配置
        if not hasattr(config, 'database') or config.database is None:
            logger.warning("数据库配置缺失，无法查询表结构")
            return []

        db = config.database
        required_attrs = ['host', 'port', 'user', 'password', 'database']
        for attr in required_attrs:
            if not hasattr(db, attr) or getattr(db, attr) is None:
                logger.warning(f"数据库配置缺失: database.{attr}")
                return []

        conn = mysql.connector.connect(
            host=db.host,
            port=db.port,
            user=db.user,
            password=db.password,
            database=db.database,
            charset=getattr(db, 'charset', 'utf8mb4'),
        )

        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = cursor.fetchall()
            if columns:
                logger.debug(f"从数据库实时获取表 {table_name} 的结构，共 {len(columns)} 个字段")
                return columns
            return []
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.warning(f"查询表结构失败: {table_name}, 错误: {e}")
        return []


def _build_intermediate_table_context(
    *,
    depends_on: list[str],
    step_results: Mapping[str, StepResult],
    materialized_schemas: Mapping[str, dict[str, object]] | None = None,
) -> tuple[str, str]:
    """构建依赖表信息与上游临时表结构说明。

    三层回退机制：
    1. materialized_schemas（Executor 写入的缓存，最准确）
    2. step_results（执行结果中的 columns 字段）
    3. MySQL 实时查询（兜底方案）
    """
    input_tables_info = ""
    intermediate_table_schemas = ""
    schemas = materialized_schemas or {}

    for dep_step_id in depends_on:
        # 优先从 materialized_schemas 获取表名和结构
        output_table = None
        columns: list[dict[str, object]] = []
        
        if dep_step_id in schemas:
            schema_info = schemas[dep_step_id]
            output_table = schema_info.get("table_name")
            cols = schema_info.get("columns", [])
            if isinstance(cols, list):
                columns = cols
        
        # 回退到 step_results
        if not output_table and dep_step_id in step_results:
            output_table = step_results[dep_step_id].get("output_table")
        
        if not output_table:
            output_table = f"step_{dep_step_id}_output"
        
        input_tables_info += f"\n- 依赖步骤 {dep_step_id} 的输出表: {output_table}"

        # 第二层回退：从 step_results 获取
        if not columns and dep_step_id in step_results:
            dep_result = step_results[dep_step_id]
            cols = dep_result.get("columns", [])
            if isinstance(cols, list):
                columns = cols

        # 第三层回退：从 MySQL 实时查询（兜底方案）
        if not columns and output_table:
            import logging
            db_columns = _fetch_table_schema_from_db(output_table)
            if db_columns:
                columns = db_columns
                logging.getLogger(__name__).info(
                    f"Schema 兜底成功: 从数据库获取表 {output_table} 的结构，共 {len(columns)} 个字段"
                )

        if columns:
            intermediate_table_schemas += f"\n\n表 `{output_table}` 的字段结构:"
            for col in columns[:12]:
                if isinstance(col, dict):
                    field_name = col.get("Field") or col.get("name") or "unknown"
                    field_type = col.get("Type") or col.get("type") or "unknown"
                    intermediate_table_schemas += f"\n  - {field_name}: {field_type}"
            if len(columns) > 12:
                intermediate_table_schemas += f"\n  ... (还有 {len(columns) - 12} 个字段)"
        else:
            # 完全无法获取结构，给出明确的警告和保守建议
            intermediate_table_schemas += (
                f"\n\n表 `{output_table}`: ⚠️ 无法获取字段结构\n"
                f"  建议：使用 SELECT * 先探查表结构，或先执行 DESCRIBE `{output_table}` 确认字段\n"
                f"  注意：禁止假设或猜测字段名，必须基于实际查询结果"
            )

    return input_tables_info, intermediate_table_schemas


def _call_llm_api(prompt: str) -> str:
    """调用LLM API生成SQL"""
    import requests
    
    # 验证配置完整性
    if not hasattr(config, 'llm') or config.llm is None:
        raise ValueError("LLM配置缺失: config.llm 未配置")
    
    llm_config = config.llm
    required_attrs = ['api_base', 'model_name', 'api_key']
    for attr in required_attrs:
        if not hasattr(llm_config, attr) or getattr(llm_config, attr) is None:
            raise ValueError(f"LLM配置缺失: config.llm.{attr} 未配置")
    
    url = f"{config.llm.api_base}/chat/completions"
    
    payload = {
        "model": config.llm.model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0,
        "max_tokens": 2048
    }
    
    headers = {
        "Authorization": f"Bearer {config.llm.api_key}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=60)
    response.raise_for_status()
    
    result_obj = cast(object, response.json())
    if not isinstance(result_obj, dict):
        raise ValueError("LLM返回格式错误: 顶层不是JSON对象")
    result: dict[str, object] = result_obj

    choices = result.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("LLM返回格式错误: 缺少choices")

    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise ValueError("LLM返回格式错误: choices[0]不是对象")

    message = first_choice.get("message")
    if not isinstance(message, dict):
        raise ValueError("LLM返回格式错误: 缺少message")

    content = message.get("content")
    if not isinstance(content, str):
        raise ValueError("LLM返回格式错误: message.content不是字符串")

    return content


def _clean_sql(sql: str) -> str:
    """清理SQL字符串，尽量提取可执行SQL片段。"""
    text = sql.strip()
    if not text:
        return ""

    tagged_sql = _extract_tagged_sql(text)
    if tagged_sql:
        text = tagged_sql

    # 移除 markdown 代码块
    if text.startswith("```sql"):
        text = text[6:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]

    # 统一行并去空行
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    text = "\n".join(lines).strip()

    # 提取第一个 CREATE TABLE 或 SELECT 开始的片段
    match = re.search(r"(?is)\b(CREATE\s+TABLE|WITH|SELECT)\b", text)
    if not match:
        return text

    candidate = text[match.start():].strip()

    # 如果有分号，截断到第一条语句结束
    semicolon_idx = candidate.find(";")
    if semicolon_idx >= 0:
        candidate = candidate[: semicolon_idx + 1].strip()
        return candidate

    # 无分号时，按行裁剪尾部解释文本
    return _trim_non_sql_trailing_lines(candidate)


def _looks_like_sql(sql: str) -> bool:
    """判断字符串是否为可执行 SQL 片段。"""
    sql_upper = sql.strip().upper()
    return (
        sql_upper.startswith("SELECT")
        or sql_upper.startswith("WITH")
        or sql_upper.startswith("CREATE TABLE")
    )


def _extract_tagged_sql(text: str) -> str:
    """提取 <SQL>...</SQL> 或 ```sql ...``` 包裹的内容。"""
    tag_match = re.search(r"(?is)<sql>\s*(.*?)\s*</sql>", text)
    if tag_match:
        return tag_match.group(1).strip()

    fenced_match = re.search(r"(?is)```sql\s*(.*?)\s*```", text)
    if fenced_match:
        return fenced_match.group(1).strip()

    return ""


def _trim_non_sql_trailing_lines(candidate: str) -> str:
    """无分号时，尽量保留 SQL 主体并剔除尾部解释文本。"""
    lines = [line.strip() for line in candidate.split("\n") if line.strip()]
    if not lines:
        return candidate.strip()

    sql_line_pattern = re.compile(
        r"^(SELECT|FROM|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|"
        r"INNER\s+JOIN|OUTER\s+JOIN|ON|UNION|WITH|AS|CREATE\s+TABLE|INSERT\s+INTO|VALUES|"
        r"AND|OR|CASE|WHEN|THEN|ELSE|END|,|\)|\()",
        re.IGNORECASE,
    )

    kept: list[str] = []
    for idx, line in enumerate(lines):
        if idx == 0:
            kept.append(line)
            continue

        if sql_line_pattern.match(line):
            kept.append(line)
            continue

        # 行内仍可能是 SQL 续写（例如 "score DESC"）
        if re.match(r"^[a-zA-Z_][\w\.]*\s*(=|>|<|>=|<=|LIKE|IN|BETWEEN|DESC|ASC)\b", line, re.IGNORECASE):
            kept.append(line)
            continue

        # 非 SQL 风格行，视为解释文本开始，停止
        break

    return "\n".join(kept).strip()


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in cast(list[object], value)]


def _coerce_filters(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    raw_list: list[object] = value
    normalized: list[dict[str, object]] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        item_map: dict[object, object] = item
        row: dict[str, object] = {}
        for key, val in item_map.items():
            row[str(key)] = val
        if row:
            normalized.append(row)
    return normalized


def _format_filter_hint(filters: list[dict[str, object]]) -> str:
    if not filters:
        return "（未提供显式 filters；请从步骤描述推断必要条件）"

    lines: list[str] = []
    for item in filters[:10]:
        field = str(item.get("field", ""))
        op = str(item.get("operator", ""))
        value = item.get("value")
        lines.append(f"- field={field}, operator={op}, value={value}")
    if len(filters) > 10:
        lines.append(f"- ... 还有 {len(filters) - 10} 条")
    return "\n".join(lines)


def _format_schema_block(
    *,
    schema_context: str,
    depends_on: list[str],
    has_intermediate_schema: bool,
) -> str:
    """构建 schema 提示块：非首步优先上游中间表，弱化完整原始 schema。"""
    schema_text = schema_context or "（Schema信息未提供）"

    # 第一步或没有上游结构时，保留完整 schema
    if not depends_on or not has_intermediate_schema:
        return schema_text

    # 非首步：仅提供缩略原始 schema，避免干扰上游中间表字段
    max_chars = 1600
    snippet = schema_text[:max_chars]
    if len(schema_text) > max_chars:
        snippet += "\n...（原始Schema已截断，当前步骤应优先依赖上游临时表结构）"
    return (
        "（当前步骤已有上游中间表输入，请优先使用上游临时表结构；"
        "仅在必须直接访问原始表时参考以下原始Schema摘要）\n"
        + snippet
    )


def _short_error(error_value: object) -> str:
    if error_value is None:
        return "未知错误"
    return str(error_value)[:200]


def _extract_planner_feedback(
    *,
    current_step_id: str,
    planner_observations: object,
    loop_decision: object,
    execution_history: object | None = None,
) -> str:
    """提取 planner 输出给 SQL 生成器的错误/调整反馈（简化版）"""
    lines: list[str] = []

    if isinstance(loop_decision, dict):
        decision = str(loop_decision.get("decision") or "").strip()
        reason = str(loop_decision.get("reason") or "").strip()
        if decision and reason:
            lines.append(f"Planner决定: {decision}，原因: {reason}")

    if isinstance(execution_history, list):
        latest_failed: dict[str, object] | None = None
        for item in execution_history:
            if not isinstance(item, dict):
                continue
            if str(item.get("step_id") or "") != current_step_id:
                continue
            if str(item.get("status") or "") != "failed":
                continue
            latest_failed = item

        if latest_failed is not None:
            raw_error = str(latest_failed.get("error") or "").strip()
            if raw_error:
                lines.append(f"MySQL错误: {raw_error}")

    return "\n".join(lines) if lines else "（无）"


def _is_final_step_in_plan(current_step_id: str, plan_nodes: list[dict[str, object]]) -> bool:
    """
    判断当前步骤是否为计划中的最后一步。
    
    Args:
        current_step_id: 当前步骤ID
        plan_nodes: 计划节点列表
        
    Returns:
        如果是最后一步返回 True，否则返回 False
    """
    if not plan_nodes:
        return True  # 没有计划节点时，假设是最后一步
    
    # 找到计划列表中最后一个节点
    last_node = plan_nodes[-1]
    last_step_id = last_node.get("step_id")
    
    return str(current_step_id) == str(last_step_id)


# 默认导出
__all__ = ["create_metric_sql_generator"]
