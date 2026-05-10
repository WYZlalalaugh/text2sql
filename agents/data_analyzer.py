"""
数据分析智能体 - 生成 Python 分析代码 (Code-Based 模式)

职责:
1. 接收 Query Planner 的计划和数据库 Schema
2. 调用 LLM 生成包含 SQL 查询的 Python 代码
3. 将代码存入 State，供 python_executor 节点执行

注意:
- 代码执行由 python_executor 节点负责
- 本节点只负责代码生成
- Code-Based 模式下不依赖 CSV 文件
"""
from typing import Protocol, cast

from state import AgentState
from prompts.data_analyzer_prompt import build_data_analyzer_prompt
from agents.python_executor import clean_code
from tools.schema_cache import get_metrics


class AnalyzerLLM(Protocol):
    def invoke(self, prompt: str) -> object:
        ...


class LLMResponseWithContent(Protocol):
    content: str


def create_data_analyzer(llm_client: AnalyzerLLM):
    """
    创建数据分析智能体
    
    Args:
        llm_client: LLM 客户端, 用于生成 Python 代码
    """
    
    def data_analyzer_node(state: AgentState) -> dict[str, object]:
        """
        数据分析代码生成节点 (Code-Based 模式)
        
        读取:
        - user_query: 用户原始查询
        - schema_context: 数据库 Schema (从 context_assembler 获取)
        - query_plan: 查询规划 (从 query_planner 获取)
        - verification_feedback: 验证反馈 (重试时)
        
        写入:
        - analysis_code: 生成的 Python 代码
        - analysis_error: 错误信息 (如有)
        """
        user_query = state.get("user_query", "")
        schema_context = state.get("schema_context") or ""
        query_plan = state.get("query_plan", {})
        selected_metrics = state.get("selected_metrics", [])
        verification_feedback = state.get("verification_feedback")

        # 加载指标定义
        metrics_definitions = get_metrics()
        
        # 使用新的 Code-Based Prompt 构建函数
        prompt = build_data_analyzer_prompt(
            user_query=user_query,
            schema_context=schema_context,
            query_plan=query_plan,
            verification_feedback=verification_feedback,
            selected_metrics=selected_metrics,
            metrics_definitions=metrics_definitions
        )
        
        try:
            # 调用 LLM 生成 Python 代码
            response = llm_client.invoke(prompt)
            if hasattr(response, "content"):
                response_with_content = cast(LLMResponseWithContent, response)
                generated_code = response_with_content.content
            else:
                generated_code = str(response)
            
            # 清理代码 (移除 markdown 标记)
            generated_code = clean_code(generated_code)
            
            if not generated_code.strip():
                return {
                    "analysis_code": "",
                    "analysis_result": None,
                    "analysis_error": "LLM 未能生成有效的分析代码",
                    "current_node": "data_analyzer"
                }
            
            # 存储代码，不执行
            # 执行将由 python_executor 节点完成
            return {
                "analysis_code": generated_code,
                "analysis_result": None,
                "analysis_error": None,
                "current_node": "data_analyzer"
            }
            
        except Exception as e:
            return {
                "analysis_code": "",
                "analysis_result": None,
                "analysis_error": f"代码生成失败: {str(e)}",
                "current_node": "data_analyzer"
            }
    
    return data_analyzer_node


# 默认节点
data_analyzer_node = None
