"""
意图分类智能体 - 使用 LLM 分析用户查询意图
"""
import json
from typing import Dict, Any

from state import AgentState, IntentType, MetricInfo
from tools.schema_cache import get_metrics_summary

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
        
        # 如果用户已提供澄清回复，将其合并到查询中
        # 然后正常走 LLM 分类流程，让模型重新判断意图
        if clarification_response:
            user_query = f"{user_query} (用户补充: {clarification_response})"

        
        # 提取历史对话用于重写 (方案 A)
        messages = state.get("messages", [])
        history_text = "无"
        if len(messages) > 1:
            # 这里的 messages 包含了当前的 HumanMessage（最后一个）
            # 我们提取之前的对话作为上下文
            recent_messages = messages[:-1][-6:]  # 取最近3轮完整对话
            history_lines = []
            for m in recent_messages:
                # 处理不同格式的消息对象
                role = "User" if m.type == "human" else "Assistant"
                content = m.content
                history_lines.append(f"{role}: {content}")
            history_text = "\n".join(history_lines)
        
        # 加载精简指标摘要 (仅一级名称+描述，大幅减少 token)
        metrics_summary = get_metrics_summary()
        
        # 使用 PromptBuilder 构建提示词 (注入历史)
        prompt = prompt_builder.build_intent_classification_prompt(
            query=user_query,
            chat_history=history_text,
            full_metrics_context=metrics_summary
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
                "value_query": IntentType.VALUE_QUERY,
                "metric_query": IntentType.METRIC_QUERY,
                "metric_definition": IntentType.METRIC_DEFINITION,
                "chitchat": IntentType.CHITCHAT
            }
            intent_type = intent_type_map.get(intent_type_str, IntentType.CHITCHAT)
            
            # 使用改写后的意图重新赋值 user_query (方案 A: 直接覆盖)
            # 这样下游节点可以直接消费最清晰的 Query，无需感知多轮逻辑
            final_query = result.get("refined_intent", user_query)
            
            return {
                "intent_type": intent_type,
                "intent_analysis": result.get("analysis", ""),
                "user_query": final_query,               # 正式覆盖原始 user_query
                "correction_count": 0,                   # 初始化计数器
                "current_node": "intent_classifier"
            }
            
        except json.JSONDecodeError:
            return {
                "intent_type": IntentType.CHITCHAT,
                "intent_analysis": "无法解析 LLM 响应",
                "correction_count": 0,
                "current_node": "intent_classifier"
            }


    
    return intent_classifier_node
