"""
查询规划智能体 - 使用 LLM 生成结构化查询计划 (Reasoning Plan)
"""
import json
import logging
from typing import Dict, Any, Optional

from state import AgentState, IntentType
from tools.schema_cache import get_schema_text, get_metrics_text

logger = logging.getLogger(__name__)
from prompts.query_planner_prompt import (
    METRIC_QUERY_PLANNER_SYSTEM_PROMPT,
    METRIC_QUERY_PLANNER_PROMPT_TEMPLATE,
    SIMPLE_QUERY_PLANNER_SYSTEM_PROMPT,
    SIMPLE_QUERY_PLANNER_PROMPT_TEMPLATE
)




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
        schema = get_schema_text()
        
        # 根据意图类型选择不同的提示词
        if intent_type == IntentType.METRIC_QUERY:
            # 指标类查询：使用包含指标体系的提示词
            full_metrics = get_metrics_text()
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
        query_plan = _parse_plan_json(response_text)
        
        # 校验计划质量：必须有 reasoning_steps 或 target_fields/selected_metrics
        if not _is_plan_valid(query_plan, intent_type):
            # 一次重试机会：告知 LLM 上次输出无效
            logger.warning("Query planner 首次输出无效，尝试重试...")
            retry_suffix = "\n\n⚠️ 你上次的输出未能被正确解析为 JSON，或缺少必填字段。请严格按照输出格式要求重新输出 JSON。"
            retry_response = llm_client.invoke(prompt + retry_suffix)
            retry_text = retry_response.content if hasattr(retry_response, 'content') else str(retry_response)
            query_plan = _parse_plan_json(retry_text)
            
            if not _is_plan_valid(query_plan, intent_type):
                logger.error("Query planner 重试后仍然无效，标记为规划失败")
                return {
                    "query_plan": {},
                    "reasoning_plan": "",
                    "selected_metrics": [],
                    "target_fields": [],
                    "planning_error": "查询规划失败：LLM 未能生成有效的执行计划",
                    "current_node": "query_planner"
                }
        
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


def _parse_plan_json(text: str) -> dict:
    """从 LLM 文本响应中提取 JSON 对象"""
    try:
        json_start = text.find('{')
        json_end = text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            return json.loads(text[json_start:json_end])
        return {}
    except json.JSONDecodeError:
        return {}


def _is_plan_valid(plan: dict, intent_type) -> bool:
    """
    检查查询计划是否包含足够信息供下游节点使用
    
    METRIC_QUERY 需要: selected_metrics 或 reasoning_steps
    VALUE_QUERY 需要: target_fields 或 reasoning_steps
    """
    if not plan:
        return False
    has_reasoning = bool(plan.get("reasoning_steps"))
    if intent_type == IntentType.METRIC_QUERY:
        return has_reasoning or bool(plan.get("selected_metrics"))
    else:
        return has_reasoning or bool(plan.get("target_fields"))
