"""
SQL 纠错 Agent - 当 SQL 执行失败时调用

参考 WrenAI 的 SQL Correction Pipeline 设计。
"""
import json
from typing import Dict, Any

from state import AgentState
from prompts.sql_correction_prompt import (
    get_sql_correction_system_prompt,
    build_sql_correction_prompt
)
from prompts.sql_rules import DatabaseType


def create_sql_corrector(llm_client, database_type: DatabaseType = DatabaseType.MYSQL):
    """
    创建 SQL 纠错节点
    
    Args:
        llm_client: LLM 客户端
        database_type: 数据库类型
    """
    
    def sql_corrector_node(state: AgentState) -> Dict[str, Any]:
        """
        SQL 纠错/反思节点 (ReAct 范式)
        """
        user_query = state.get("user_query", "")
        generated_sql = state.get("generated_sql", "")
        observation = state.get("execution_observation", "")
        assembled_prompt = state.get("assembled_prompt", "")
        
        # 增加计数（放在最前面，确保无论结果如何，机会都消耗了）
        correction_count = state.get("correction_count", 0) + 1
        
        # 如果没有 SQL，无法进行反思
        if not generated_sql:
            return {
                "current_node": "sql_corrector",
                "correction_attempted": False,
                "correction_count": correction_count
            }
        
        try:
            # 提取上下文信息
            schema = _extract_schema_from_prompt(assembled_prompt)
            metric_context = _extract_metric_context_from_prompt(assembled_prompt)
            
            # 构建 ReAct 纠错提示词
            correction_prompt = build_sql_correction_prompt(
                user_query=user_query,
                invalid_sql=generated_sql,
                observation=observation,
                schema=schema,
                metric_context=metric_context
            )
            
            # 获取提示词并调用 LLM
            system_prompt = get_sql_correction_system_prompt(database_type)
            if hasattr(llm_client, 'invoke_with_system'):
                response = llm_client.invoke_with_system(system_prompt=system_prompt, user_prompt=correction_prompt)
            else:
                response = llm_client.invoke(f"{system_prompt}\n\n{correction_prompt}")
            
            response_text = response.content if hasattr(response, 'content') else str(response)
            reflection, corrected_sql = _extract_reflection_and_sql(response_text)
            
            return {
                "generated_sql": corrected_sql,
                "sql_reflection": reflection,
                "correction_attempted": True,
                "correction_count": correction_count,
                "execution_error": None,
                "current_node": "sql_corrector"
            }
            
        except Exception as e:
            return {
                "execution_error": f"SQL 纠错反思请求失败 (可能由于 Token/额度限制): {str(e)}",
                "correction_attempted": True,
                "correction_count": correction_count, # 关键：报错也要返回增加后的计数
                "current_node": "sql_corrector"
            }

    
    return sql_corrector_node


def _extract_reflection_and_sql(response_text: str) -> tuple[str, str]:
    """从 LLM 响应中提取反思过程和 SQL"""
    reflection = ""
    sql = ""
    
    try:
        # 查找 JSON 代码块或直接查找括号
        start = response_text.find('{')
        end = response_text.rfind('}') + 1
        if start >= 0 and end > start:
            json_str = response_text[start:end]
            data = json.loads(json_str)
            reflection = data.get("reflection", "")
            sql = data.get("sql", "")
            
        if sql:
            return reflection, _clean_sql(sql)
    except:
        pass

    # 如果 JSON 解析失败，尝试正则表达式匹配 (兜底)
    import re
    sql_match = re.search(r"```sql\n(.*?)\n```", response_text, re.DOTALL)
    if sql_match:
        sql = sql_match.group(1).strip()
    
    return "无法解析详细反思过程，请参考新生成的 SQL。", sql or response_text



def _extract_schema_from_prompt(assembled_prompt: str) -> str:
    """从组装的提示词中提取 Schema"""
    # 简单实现：查找包含 "Schema" 的部分
    if "### 数据库 Schema" in assembled_prompt:
        start = assembled_prompt.find("### 数据库 Schema")
        end = assembled_prompt.find("###", start + 20)
        if end > start:
            return assembled_prompt[start:end]
    return ""


def _extract_metric_context_from_prompt(assembled_prompt: str) -> str:
    """从组装的提示词中提取指标上下文"""
    if "### 指标上下文" in assembled_prompt or "### 相关指标信息" in assembled_prompt:
        marker = "### 指标上下文" if "### 指标上下文" in assembled_prompt else "### 相关指标信息"
        start = assembled_prompt.find(marker)
        end = assembled_prompt.find("###", start + 20)
        if end > start:
            return assembled_prompt[start:end]
    return ""


def _extract_sql_from_response(response_text: str) -> str:
    """从 LLM 响应中提取 SQL"""
    # 尝试解析 JSON 格式
    try:
        # 查找 JSON 块
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            data = json.loads(json_str)
            sql = data.get("sql", "")
            if sql:
                return _clean_sql(sql)
    except:
        pass
    
    # 如果 JSON 解析失败，尝试提取 SQL 代码块
    if "```sql" in response_text:
        start = response_text.find("```sql") + 6
        end = response_text.find("```", start)
        if end > start:
            return response_text[start:end].strip()
    elif "```" in response_text:
        start = response_text.find("```") + 3
        end = response_text.find("```", start)
        if end > start:
            return response_text[start:end].strip()
    
    # 最后尝试：返回整个响应（可能就是 SQL）
    return _clean_sql(response_text)


def _clean_sql(sql: str) -> str:
    """清理 SQL 字符串"""
    sql = sql.strip()
    
    # 移除可能的 markdown 标记
    if sql.startswith("```sql"):
        sql = sql[6:]
    elif sql.startswith("```"):
        sql = sql[3:]
    
    if sql.endswith("```"):
        sql = sql[:-3]
    
    return sql.strip()


# 默认节点（需要运行时注入 LLM 客户端）
sql_corrector_node = None
