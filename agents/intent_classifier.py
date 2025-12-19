"""
意图分类智能体 - 使用 LLM 分析用户查询意图
"""
import json
import os
from typing import Dict, Any

from state import AgentState, IntentType, MetricInfo
from config import config

def create_intent_classifier(llm_client, prompt_builder):
    """
    创建意图分类节点函数
    
    Args:
        llm_client: LLM 客户端，需要有 invoke 方法
        prompt_builder: 提示词构建器
    """
    
    def intent_classifier_node(state: AgentState) -> Dict[str, Any]:
        """意图分类节点"""
        user_query = state.get("user_query", "")
        clarification_response = state.get("clarification_response", "")
        
        # 关键：如果用户已提供澄清回复，保持原意图为 metric_query，跳过重新分类
        # 关键：如果用户已提供澄清回复，保持原意图为 metric_query，但需要合并意图
        if clarification_response:
            # 简单合并策略：将澄清回复作为上下文附加到原始查询后
            refined_intent = f"{user_query} (用户补充: {clarification_response})"
            
            # 尝试从状态中保留原始意图类型
            original_intent = state.get("intent_type") or IntentType.METRIC_QUERY
            
            return {
                "intent_type": original_intent,          # 保持原始意图
                "refined_intent": refined_intent,        # 传递合并后的明确意图
                "intent_analysis": f"用户提供了澄清回复: {clarification_response}",
                "correction_count": 0,                   # 初始化计数器
                "current_node": "intent_classifier"
            }

        
        # 加载全量指标体系 (不再使用 matched_metrics)
        full_metrics_text = ""
        metrics_path = config.paths.metrics_path
        if os.path.exists(metrics_path):
            with open(metrics_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    full_metrics_text = json.dumps(data, ensure_ascii=False, indent=2)
                except:
                    pass
        
        # 使用 PromptBuilder 构建提示词
        prompt = prompt_builder.build_intent_classification_prompt(
            query=user_query,
            full_metrics_context=full_metrics_text
        )
        
        # 调用 LLM
        response = llm_client.invoke(prompt)
        
        # 解析响应
        try:
            # 提取 JSON
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            # 尝试找到 JSON 块
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                result = json.loads(json_str)
            else:
                try:
                    result = json.loads(response_text)
                except:
                    result = {"intent_type": "chitchat", "analysis": "解析失败"}
            
            # 转换意图类型
            intent_type_str = result.get("intent_type", "chitchat")
            intent_type_map = {
                "value_query": IntentType.VALUE_QUERY,      # 新增
                "simple_query": IntentType.VALUE_QUERY,     # 兼容旧名称
                "metric_query": IntentType.METRIC_QUERY,
                "metric_definition": IntentType.METRIC_DEFINITION,
                "chitchat": IntentType.CHITCHAT
            }
            intent_type = intent_type_map.get(intent_type_str, IntentType.CHITCHAT)
            
            return {
                "intent_type": intent_type,
                "intent_analysis": result.get("analysis", ""),
                "correction_count": 0,                   # 初始化计数器
                "current_node": "intent_classifier"
            }

            
        except json.JSONDecodeError:
            return {
                "intent_type": IntentType.CHITCHAT,
                "intent_analysis": "无法解析 LLM 响应",
                "correction_count": 0,                   # 初始化计数器
                "current_node": "intent_classifier"
            }

    
    return intent_classifier_node
