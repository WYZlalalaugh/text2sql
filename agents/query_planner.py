"""
查询规划智能体 - 使用 LLM 生成结构化查询计划 (Reasoning Plan)
"""
import json
import os
from typing import Dict, Any, Optional

from state import AgentState, IntentType
from config import config
from prompts.query_planner_prompt import (
    METRIC_QUERY_PLANNER_SYSTEM_PROMPT,
    METRIC_QUERY_PLANNER_PROMPT_TEMPLATE,
    SIMPLE_QUERY_PLANNER_SYSTEM_PROMPT,
    SIMPLE_QUERY_PLANNER_PROMPT_TEMPLATE
)


def load_full_metrics() -> str:
    """加载完整指标体系"""
    metrics_path = config.paths.metrics_path
    if os.path.exists(metrics_path):
        with open(metrics_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                return json.dumps(data, ensure_ascii=False, indent=2)
            except:
                return "{}"
    return "{}"


def load_schema() -> str:
    """加载数据库 Schema"""
    schema_path = config.paths.schema_path
    if os.path.exists(schema_path):
        with open(schema_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                return json.dumps(data, ensure_ascii=False, indent=2)
            except:
                return "{}"
    return "{}"


def create_query_planner(llm_client):
    """
    创建查询规划节点
    
    Args:
        llm_client: LLM 客户端
    """
    
    def query_planner_node(state: AgentState) -> Dict[str, Any]:
        """查询规划节点 - 根据查询类型使用不同的提示词"""
        user_query = state.get("user_query", "")
        refined_intent = state.get("refined_intent") or user_query
        intent_type = state.get("intent_type", IntentType.SIMPLE_QUERY)
        
        # 加载 Schema（两种查询都需要）
        schema = load_schema()
        
        # 根据意图类型选择不同的提示词
        if intent_type == IntentType.METRIC_QUERY:
            # 指标类查询：使用包含指标体系的提示词
            full_metrics = load_full_metrics()
            prompt = METRIC_QUERY_PLANNER_PROMPT_TEMPLATE.format(
                system_prompt=METRIC_QUERY_PLANNER_SYSTEM_PROMPT,
                metrics=full_metrics,
                schema=schema,
                query=refined_intent
            )
        else:
            # 普通查询：使用不包含指标的提示词
            prompt = SIMPLE_QUERY_PLANNER_PROMPT_TEMPLATE.format(
                system_prompt=SIMPLE_QUERY_PLANNER_SYSTEM_PROMPT,
                schema=schema,
                query=refined_intent
            )
        
        # 调用 LLM
        response = llm_client.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 解析 JSON 响应
        try:
            # 尝试提取 JSON
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                query_plan = json.loads(json_str)
            else:
                query_plan = {}
        except json.JSONDecodeError:
            query_plan = {}
        
        # 提取 reasoning_steps 作为文本
        reasoning_steps = query_plan.get("reasoning_steps", [])
        reasoning_plan_text = "\n".join(reasoning_steps) if reasoning_steps else ""
        
        # 提取 selected_metrics（仅指标查询有此字段）
        selected_metrics = query_plan.get("selected_metrics", [])
        
        # 提取 target_fields（仅普通查询有此字段）
        target_fields = query_plan.get("target_fields", [])
        
        return {
            "query_plan": query_plan,
            "reasoning_plan": reasoning_plan_text,
            "selected_metrics": selected_metrics,
            "target_fields": target_fields,
            "current_node": "query_planner"
        }
    
    return query_planner_node
