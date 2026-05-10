"""
SQL 生成节点 - 调用微调模型生成 SQL
"""
import requests
import json
import re
from typing import Dict, Any

from state import AgentState
from config import config


def create_sql_generator(model_client=None):
    """
    创建 SQL 生成节点
    
    Args:
        model_client: 可选的模型客户端，如果不提供则使用 Ollama API
    """
    
    def sql_generator_node(state: AgentState) -> Dict[str, Any]:
        """SQL 生成节点 - 调用微调模型"""
        assembled_prompt = state.get("assembled_prompt", "")
        
        if not assembled_prompt:
            return {
                "generated_sql": "",
                "execution_error": "没有组装好的 Prompt",
                "current_node": "sql_generator"
            }
        
        try:
            if model_client is not None:
                # 使用提供的客户端
                response = model_client.invoke(assembled_prompt)
                sql = response.content if hasattr(response, 'content') else str(response)
            else:
                # 使用 Ollama API
                sql = call_ollama_api(assembled_prompt)
            
            # 清理 SQL（移除可能的 markdown 标记）
            sql = clean_sql(sql)

            if not looks_like_sql(sql):
                return {
                    "generated_sql": "",
                    "execution_error": "SQL 生成失败: 模型返回包含非SQL内容或SQL格式无效",
                    "current_node": "sql_generator",
                }
            
            return {
                "generated_sql": sql,
                "current_node": "sql_generator"
            }
            
        except Exception as e:
            return {
                "generated_sql": "",
                "execution_error": f"SQL 生成失败: {str(e)}",
                "current_node": "sql_generator"
            }
    
    return sql_generator_node


def call_ollama_api(prompt: str) -> str:
    """调用 Ollama API 生成 SQL"""
    url = f"{config.finetuned_model.api_base}/api/generate"
    
    payload = {
        "model": config.finetuned_model.model_name,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": config.finetuned_model.temperature,
            "num_predict": config.finetuned_model.max_tokens,
            "stop": ["</SQL>"]
        }
    }
    
    response = requests.post(url, json=payload, timeout=60)
    response.raise_for_status()
    
    result = response.json()
    return result.get("response", "")


def clean_sql(sql: str) -> str:
    """清理 SQL 字符串，仅保留第一条可执行 SQL 语句。"""
    sql = sql.strip()
    if not sql:
        return ""

    tagged = _extract_tagged_sql(sql)
    if tagged:
        sql = tagged
    
    # 移除 markdown 代码块标记
    if sql.startswith("```sql"):
        sql = sql[6:]
    elif sql.startswith("```"):
        sql = sql[3:]
    
    if sql.endswith("```"):
        sql = sql[:-3]

    sql = "\n".join([line.strip() for line in sql.split("\n") if line.strip()]).strip()

    match = re.search(r"(?is)\b(WITH|SELECT|CREATE\s+TABLE)\b", sql)
    if not match:
        return sql

    candidate = sql[match.start():].strip()
    semicolon_idx = candidate.find(";")
    if semicolon_idx >= 0:
        return candidate[: semicolon_idx + 1].strip()

    lines = [line.strip() for line in candidate.split("\n") if line.strip()]
    if not lines:
        return candidate

    sql_line_pattern = re.compile(
        r"^(SELECT|FROM|WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|"
        r"INNER\s+JOIN|OUTER\s+JOIN|ON|UNION|WITH|AS|CREATE\s+TABLE|AND|OR|CASE|WHEN|THEN|ELSE|END|,|\)|\(|"
        r"INSERT\s+INTO|VALUES)",
        re.IGNORECASE,
    )
    kept: list[str] = []
    for idx, line in enumerate(lines):
        if idx == 0 or sql_line_pattern.match(line):
            kept.append(line)
            continue
        if re.match(r"^[a-zA-Z_][\w\.]*\s*(=|>|<|>=|<=|LIKE|IN|BETWEEN|DESC|ASC)\b", line, re.IGNORECASE):
            kept.append(line)
            continue
        break

    return "\n".join(kept).strip()


def _extract_tagged_sql(text: str) -> str:
    """提取 <SQL>...</SQL> 或 ```sql ...``` 包裹的内容。"""
    tag_match = re.search(r"(?is)<sql>\s*(.*?)\s*</sql>", text)
    if tag_match:
        return tag_match.group(1).strip()

    fenced_match = re.search(r"(?is)```sql\s*(.*?)\s*```", text)
    if fenced_match:
        return fenced_match.group(1).strip()

    return ""


def looks_like_sql(sql: str) -> bool:
    """判断是否为可执行 SQL 片段。"""
    sql_upper = sql.strip().upper()
    if not sql_upper:
        return False
    return (
        sql_upper.startswith("SELECT")
        or sql_upper.startswith("WITH")
        or sql_upper.startswith("CREATE TABLE")
    )


# 默认节点
sql_generator_node = None
