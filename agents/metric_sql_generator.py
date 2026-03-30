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

        raw_schema_context = state.get("schema_context")
        schema_context = raw_schema_context if isinstance(raw_schema_context, str) else ""
        
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
        
        try:
            # 构建提示词
            if not failed_attempts:
                # 首次尝试
                prompt = _build_normal_prompt(
                    current_node, schema_context, step_results, is_final_step
                )
            else:
                # 重试 - 包含错误历史
                prompt = _build_retry_prompt(
                    current_node, schema_context, step_results, failed_attempts, is_final_step
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
    is_final_step: bool = False,
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
8. 只输出SQL代码，不要解释

请生成SQL：
"""
    return prompt


def _build_retry_prompt(
    node: MetricPlanNode,
    schema_context: str,
    step_results: Mapping[str, StepResult],
    failed_attempts: list[dict[str, object]],
    is_final_step: bool = False,
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
    )
    schema_block = _format_schema_block(
        schema_context=schema_context,
        depends_on=depends_on,
        has_intermediate_schema=bool(intermediate_table_schemas),
    )
    
    # 构建错误历史
    error_history = "\n".join([
        f"尝试 {i+1}: {_short_error(record.get('error'))}"
        for i, record in enumerate(failed_attempts)
    ])
    
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
    
    prompt = f"""你是一个SQL生成专家。之前的SQL执行失败了，请分析错误并生成修正后的SQL。

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

## 数据库Schema
```
{schema_block}
```

## 输出要求（必须对齐）
- 预期输出字段: {', '.join(expected_outputs) if expected_outputs else '根据意图推断'}
- 预期粒度: {', '.join(expected_grain) if expected_grain else '根据意图推断'}

{sql_requirement}
3. 确保SQL语法兼容MySQL 8.0
4. 查询上游临时表时，必须使用"上游临时表结构"中定义的字段名
5. 绝对不要从历史错误文本中推断新字段名或新表结构
6. 仅以"上游临时表结构"和"数据库Schema"作为字段来源
7. 文本字段筛选默认优先使用 LIKE（模糊匹配），除非用户明确要求精确匹配
8. 数值/日期字段使用精确比较（=、between 等），避免 LIKE
9. 只生成单个SQL语句

请生成修正后的SQL：
"""
    return prompt


def _build_intermediate_table_context(
    *,
    depends_on: list[str],
    step_results: Mapping[str, StepResult],
) -> tuple[str, str]:
    """构建依赖表信息与上游临时表结构说明。"""
    input_tables_info = ""
    intermediate_table_schemas = ""

    for dep_step_id in depends_on:
        if dep_step_id in step_results:
            dep_result = step_results[dep_step_id]
            output_table = dep_result.get("output_table") or f"step_{dep_step_id}_output"
            input_tables_info += f"\n- 依赖步骤 {dep_step_id} 的输出表: {output_table}"

            columns_raw = dep_result.get("columns", [])
            columns = cast(list[dict[str, object]], columns_raw) if isinstance(columns_raw, list) else []
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
            input_tables_info += f"\n- 依赖步骤 {dep_step_id}: 表结构待查询"

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
    match = re.search(r"(?is)\b(CREATE\s+TABLE|SELECT)\b", text)
    if not match:
        return text

    candidate = text[match.start():].strip()

    # 如果包含明显解释性前缀，截到第一个 SQL 语句结束
    semicolon_idx = candidate.find(";")
    if semicolon_idx >= 0:
        candidate = candidate[: semicolon_idx + 1].strip()

    return candidate


def _looks_like_sql(sql: str) -> bool:
    """判断字符串是否为可执行 SQL 片段。"""
    sql_upper = sql.strip().upper()
    return sql_upper.startswith("SELECT") or sql_upper.startswith("CREATE TABLE")


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
