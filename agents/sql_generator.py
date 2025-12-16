"""
SQL 生成节点 - 调用微调模型生成 SQL
"""
import requests
import json
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
            "num_predict": config.finetuned_model.max_tokens
        }
    }
    
    response = requests.post(url, json=payload, timeout=60)
    response.raise_for_status()
    
    result = response.json()
    return result.get("response", "")


def clean_sql(sql: str) -> str:
    """清理 SQL 字符串"""
    sql = sql.strip()
    
    # 移除 markdown 代码块标记
    if sql.startswith("```sql"):
        sql = sql[6:]
    elif sql.startswith("```"):
        sql = sql[3:]
    
    if sql.endswith("```"):
        sql = sql[:-3]
    
    return sql.strip()


# 默认节点
sql_generator_node = None
