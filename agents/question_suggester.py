"""
问题推荐智能体 - 负责根据当前上下文产生后续追问建议
"""
import json
from typing import Dict, Any, List
from state import AgentState
from prompts.suggestion_prompt import QUESTION_SUGGESTION_PROMPT

def create_question_suggester(llm_client):
    """
    创建问题推荐节点函数
    """
    
    def question_suggester_node(state: AgentState) -> Dict[str, Any]:
        """问题推荐节点"""
        user_query = state.get("user_query", "")
        generated_sql = state.get("generated_sql", "")
        execution_result = state.get("execution_result", [])
        
        # 格式化结果摘要以节省 Token
        result_summary = ""
        if isinstance(execution_result, list) and len(execution_result) > 0:
            result_summary = json.dumps(execution_result[:5], ensure_ascii=False)
        else:
            result_summary = "无数据返回"

        # 构建提示词
        prompt = QUESTION_SUGGESTION_PROMPT.format(
            user_query=user_query,
            generated_sql=generated_sql,
            execution_result=result_summary
        )
        
        # 调用 LLM
        response = llm_client.invoke(prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 解析问题列表
        suggestions = [
            q.strip() for q in response_text.split('\n') 
            if q.strip() and len(q.strip()) > 5
        ][:3]
        
        return {
            "suggested_questions": suggestions,
            "current_node": "question_suggester"
        }
        
    return question_suggester_node
