"""
响应生成节点 - 将查询结果转换为自然语言回复
"""
import json
import os
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, Any

from state import AgentState, IntentType
from config import config
from prompts import (
    CHITCHAT_PROMPT,
    QUERY_RESULT_PROMPT,
    GREETING_RESPONSE,
    HELP_RESPONSE
)


class SQLResultEncoder(json.JSONEncoder):
    """自定义 JSON 编码器，处理 MySQL 返回的特殊类型"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        return super().default(obj)


def create_response_generator(llm_client=None):
    """
    创建响应生成节点
    
    Args:
        llm_client: LLM 客户端，用于生成自然语言回复
    """
    
    def response_generator_node(state: AgentState) -> Dict[str, Any]:
        """响应生成节点"""
        intent_type = state.get("intent_type", IntentType.CHITCHAT)
        user_query = state.get("user_query", "")
        
        # 根据意图类型处理
        if intent_type == IntentType.CHITCHAT:
            return generate_chitchat_response(state, llm_client)
        
        elif intent_type == IntentType.METRIC_DEFINITION:
            return generate_definition_response(state, llm_client)
        
        else:  # SIMPLE_QUERY 或 METRIC_QUERY
            return generate_query_response(state, llm_client)
    
    return response_generator_node


def generate_chitchat_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成闲聊回复"""
    user_query = state.get("user_query", "")
    
    if llm_client is not None:
        prompt = CHITCHAT_PROMPT.format(user_query=user_query)
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
    else:
        # 使用预设回复
        greetings = ["你好", "您好", "hi", "hello", "嗨", "早上好", "下午好", "晚上好"]
        if any(g in user_query.lower() for g in greetings):
            reply = GREETING_RESPONSE
        elif "帮助" in user_query or "help" in user_query.lower() or "怎么用" in user_query:
            reply = HELP_RESPONSE
        else:
            reply = "您好！请问有什么可以帮您的？我可以帮您查询学校数据和教育指标信息。"
    
    return {
        "final_response": reply,
        "current_node": "response_generator"
    }


def generate_definition_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成指标定义回复 - 基于全量指标体系"""
    user_query = state.get("user_query", "")
    
    # 尝试加载全量指标
    full_metrics_text = ""
    metrics_path = config.paths.metrics_path
    if os.path.exists(metrics_path):
        with open(metrics_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                full_metrics_text = json.dumps(data, ensure_ascii=False, indent=2)
            except:
                pass
    
    if llm_client and full_metrics_text:
        # 使用 LLM 生成定义解释
        prompt = f"""你是一个教育指标专家。请根据以下指标体系定义，回答用户关于指标含义的问题。

### 指标体系
```json
{full_metrics_text}
```

### 用户问题
{user_query}

请直接回答指标的定义和解释，如果用户问到了具体的计算方式，也请一并说明。
"""
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
    else:
        # Fallback 到简单的关键词匹配（如果没有 LLM 或加载失败）
        reply = "抱歉，暂时无法查询指标定义信息。"
    
    return {
        "final_response": reply,
        "current_node": "response_generator"
    }


def generate_query_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成查询结果回复"""
    user_query = state.get("user_query", "")
    generated_sql = state.get("generated_sql", "")
    execution_result = state.get("execution_result")
    execution_error = state.get("execution_error")
    
    # 如果有错误
    if execution_error:
        reply = f"查询执行遇到问题\n\n{execution_error}\n\n"
        if generated_sql:
            reply += f"生成的 SQL：\n```sql\n{generated_sql}\n```"
        return {
            "final_response": reply,
            "current_node": "response_generator"
        }
    
    # 没有结果
    if not execution_result:
        reply = "查询完成，但没有找到符合条件的数据。\n\n"
        reply += "建议您：\n"
        reply += "- 放宽查询条件重试\n"
        reply += "- 检查年份、地区等筛选条件\n"
        if generated_sql:
            reply += f"\n执行的 SQL：\n```sql\n{generated_sql}\n```"
        return {
            "final_response": reply,
            "current_node": "response_generator" 
        }
    
    # 有结果，使用 LLM 生成自然语言回复
    if llm_client is not None:
        result_str = json.dumps(execution_result[:10], ensure_ascii=False, indent=2, cls=SQLResultEncoder)
        prompt = QUERY_RESULT_PROMPT.format(
            user_query=user_query,
            result_data=result_str
        )
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
    else:
        # 简单格式化
        result_count = len(execution_result)
        reply = f"查询完成，共找到 {result_count} 条结果\n\n"
        
        # 显示前几条
        for i, row in enumerate(execution_result[:5]):
            reply += f"{i+1}. {json.dumps(row, ensure_ascii=False)}\n"
        
        if result_count > 5:
            reply += f"\n... 还有 {result_count - 5} 条结果"
    
    # 附加 SQL (可选，根据配置)
    reply += f"\n\n---\n执行的 SQL：\n```sql\n{generated_sql}\n```"
    
    return {
        "final_response": reply,
        "current_node": "response_generator"
    }


# 默认节点
response_generator_node = None
