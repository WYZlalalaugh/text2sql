"""
歧义检测与澄清智能体 - 使用 LLM 检测查询中的歧义并生成澄清问题
"""
import json
import os
from typing import Dict, Any

from state import AgentState, IntentType
from config import config

def create_ambiguity_checker(llm_client, prompt_builder):
    """
    创建歧义检测节点函数
    
    Args:
        llm_client: LLM 客户端
        prompt_builder: 提示词构建器
    """
    
    def ambiguity_checker_node(state: AgentState) -> Dict[str, Any]:
        """歧义检测节点"""
        user_query = state.get("user_query", "")
        # matched_metrics = state.get("matched_metrics", []) # 不再使用
        messages = state.get("messages", [])
        clarification_response = state.get("clarification_response", "")
        clarification_count = state.get("clarification_count", 0)
        
        # 加载全量指标体系
        full_metrics_text = ""
        metrics_path = config.paths.metrics_path
        if os.path.exists(metrics_path):
            with open(metrics_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    full_metrics_text = json.dumps(data, ensure_ascii=False, indent=2)
                except:
                    pass
        
        # 如果已经澄清过多次，直接放行
        if clarification_count >= 2:
            return {
                "ambiguity_detected": False,
                "refined_intent": user_query + (f" (澄清: {clarification_response})" if clarification_response else ""),
                "current_node": "ambiguity_checker"
            }
        
        # 关键修复：如果用户已经提供了澄清回复，直接放行，不再检测歧义
        if clarification_response:
            refined = f"{user_query} ({clarification_response})"
            return {
                "ambiguity_detected": False,
                "refined_intent": refined,
                "current_node": "ambiguity_checker"
            }
        
        # 格式化对话历史
        if messages:
            history_text = "\n".join([
                f"{m.get('role', 'user')}: {m.get('content', '')}"
                for m in messages[-4:]  # 只取最近 4 条
            ])
        else:
            history_text = "无"
        
        # 使用 PromptBuilder 构建提示词
        prompt = prompt_builder.build_ambiguity_check_prompt(
            query=user_query,
            full_metrics_context=full_metrics_text,
            conversation_history=history_text
        )
        
        # 调用 LLM
        response = llm_client.invoke(prompt)
        
        # 解析响应
        try:
            response_text = response.content if hasattr(response, 'content') else str(response)
            
            # 提取 JSON
            json_start = response_text.find('{')
            json_end = response_text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                json_str = response_text[json_start:json_end]
                result = json.loads(json_str)
            else:
                try:
                    result = json.loads(response_text)
                except:
                    result = {"ambiguity_detected": False, "refined_intent": user_query}
            
            ambiguity_detected = result.get("ambiguity_detected", False)
            
            if ambiguity_detected:
                return {
                    "ambiguity_detected": True,
                    "ambiguity_details": result.get("ambiguity_details", []),
                    "clarification_question": result.get("clarification_question", "请提供更多细节"),
                    "current_node": "ambiguity_checker",
                    "clarification_count": clarification_count + 1
                }
            else:
                return {
                    "ambiguity_detected": False,
                    "ambiguity_details": [],
                    "refined_intent": result.get("refined_intent", user_query),
                    "current_node": "ambiguity_checker"
                }
            
        except json.JSONDecodeError:
            # 解析失败，默认放行
            return {
                "ambiguity_detected": False,
                "refined_intent": user_query,
                "current_node": "ambiguity_checker"
            }
    
    return ambiguity_checker_node
