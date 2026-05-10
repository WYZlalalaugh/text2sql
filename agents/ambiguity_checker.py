"""
歧义检测与澄清智能体 - 使用 LLM 检测查询中的歧义并生成澄清问题
"""
import json
from typing import Dict, Any

from state import AgentState, IntentType
from tools.schema_cache import get_metrics_text

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
        
        # 加载全量指标体系 (使用缓存)
        full_metrics_text = get_metrics_text()
        
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
            # 重新提取指标，确保有 metrics_context
            extracted_metrics = _extract_metrics_with_llm(
                llm_client=llm_client,
                prompt_builder=prompt_builder,
                user_query=user_query,
                refined_intent=refined,
                full_metrics_text=full_metrics_text
            )
            return {
                "ambiguity_detected": False,
                "refined_intent": refined,
                "metrics_context": extracted_metrics,  # 确保传递提取的指标
                "current_node": "ambiguity_checker"
            }
        
        # 格式化对话历史
        history_text = "无"
        if messages:
            history_lines = []
            for m in messages[-4:]:  # 只取最近 4 条
                # 处理不同格式的消息对象 (LangChain Message 或 Dict)
                if hasattr(m, 'type'):
                    role = "User" if m.type == "human" else "Assistant"
                    content = m.content
                else:
                    role = m.get('role', 'user').capitalize()
                    content = m.get('content', '')
                history_lines.append(f"{role}: {content}")
            history_text = "\n".join(history_lines)
        
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
                # 无歧义时，使用 LLM 提取结构化的指标体系
                extracted_metrics = _extract_metrics_with_llm(
                    llm_client=llm_client,
                    prompt_builder=prompt_builder,
                    user_query=user_query,
                    refined_intent=result.get("refined_intent", user_query),
                    full_metrics_text=full_metrics_text
                )
                
                return {
                    "ambiguity_detected": False,
                    "ambiguity_details": [],
                    "refined_intent": result.get("refined_intent", user_query),
                    "metrics_context": extracted_metrics,  # 新增：LLM 提取的指标体系
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


def _extract_metrics_with_llm(
    llm_client,
    prompt_builder,
    user_query: str,
    refined_intent: str,
    full_metrics_text: str
) -> list[dict[str, str]]:
    """
    使用 LLM 从全量指标体系中提取与用户问题相关的指标列表。
    
    Returns:
        指标列表，每个元素是 {"一级指标": "xxx", "二级指标": "xxx"} 的字典
        例如: [{"一级指标": "基础设施", "二级指标": "网络"}, {"一级指标": "基础设施", "二级指标": "终端"}]
    """
    # 构建提取提示词
    extract_prompt = f"""你是一个指标体系提取专家。请根据用户的查询意图，从全量指标体系中提取出**仅与该查询相关的**指标。

## 用户原始查询
{user_query}

## 澄清后的精确意图
{refined_intent}

## 全量指标体系
```json
{full_metrics_text}
```

## 任务
请分析用户意图，提取出相关的**一级指标**和**二级指标**。

## 输出格式
只输出一个 JSON 数组，不要任何其他文字：
[
  {{"一级指标": "基础设施", "二级指标": "网络"}},
  {{"一级指标": "基础设施", "二级指标": "终端"}},
  {{"一级指标": "教育教学", "二级指标": "教学评价"}}
]

## 提取规则
1. **只保留与用户问题相关的指标**，不相关的指标不要出现在输出中
2. 每个指标用字典表示，包含两个键："一级指标" 和 "二级指标"
3. 如果某个一级指标下的所有二级指标都相关，列出所有二级指标
4. 如果用户问题只涉及某个一级指标的整体，列出该一级指标下所有二级指标
5. 返回合法的 JSON 数组格式，用方括号包裹

请只输出 JSON 数组："""

    try:
        # 调用 LLM
        response = llm_client.invoke(extract_prompt)
        response_text = response.content if hasattr(response, 'content') else str(response)
        
        # 提取 JSON
        json_start = response_text.find('[')
        json_end = response_text.rfind(']') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            extracted_metrics = json.loads(json_str)
            if isinstance(extracted_metrics, list):
                # 验证每个元素是否是字典且包含需要的键
                validated = []
                for item in extracted_metrics:
                    if isinstance(item, dict) and '一级指标' in item and '二级指标' in item:
                        validated.append(item)
                return validated
    except Exception as e:
        # 提取失败，返回空列表，让下游使用全量指标
        print(f"[WARNING] LLM 指标提取失败: {e}")
    
    return []
