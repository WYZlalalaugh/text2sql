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
        SQL 纠错节点
        
        接收错误的 SQL 和错误信息，生成纠正后的 SQL
        """
        generated_sql = state.get("generated_sql", "")
        execution_error = state.get("execution_error", "")
        assembled_prompt = state.get("assembled_prompt", "")
        
        # 如果没有错误或没有 SQL，直接返回
        if not execution_error or not generated_sql:
            return {
                "current_node": "sql_corrector",
                "correction_attempted": False
            }
        
        try:
            # 从 assembled_prompt 中提取 schema 和 metric_context
            # 简单处理：直接使用原始 assembled_prompt 的部分内容
            schema = _extract_schema_from_prompt(assembled_prompt)
            metric_context = _extract_metric_context_from_prompt(assembled_prompt)
            
            # 构建纠错提示词
            correction_prompt = build_sql_correction_prompt(
                invalid_sql=generated_sql,
                error_message=execution_error,
                schema=schema,
                metric_context=metric_context
            )
            
            # 获取系统提示词
            system_prompt = get_sql_correction_system_prompt(database_type)
            
            # 调用 LLM
            # 注意：这里假设 llm_client 有一个 invoke_with_system 方法
            # 如果没有，需要根据实际 LLM 客户端接口调整
            if hasattr(llm_client, 'invoke_with_system'):
                response = llm_client.invoke_with_system(
                    system_prompt=system_prompt,
                    user_prompt=correction_prompt
                )
            else:
                # 降级方案：将系统提示词和用户提示词合并
                full_prompt = f"{system_prompt}\n\n{correction_prompt}"
                response = llm_client.invoke(full_prompt)
            
            # 提取纠正后的 SQL
            response_text = response.content if hasattr(response, 'content') else str(response)
            corrected_sql = _extract_sql_from_response(response_text)
            
            # 增加纠错计数
            correction_count = state.get("correction_count", 0) + 1
            
            return {
                "generated_sql": corrected_sql,
                "correction_attempted": True,
                "correction_count": correction_count,
                "execution_error": None,  # 清除旧错误信息
                "current_node": "sql_corrector"
            }
            
        except Exception as e:
            return {
                "execution_error": f"SQL 纠错失败: {str(e)}",
                "correction_attempted": True,
                "current_node": "sql_corrector"
            }
    
    return sql_corrector_node


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
