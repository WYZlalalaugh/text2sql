"""
LangGraph 图编排 - 定义智能体工作流

支持两种主要流程:
1. VALUE_QUERY: 直接 SQL 生成 -> 执行 -> 响应
2. METRIC_QUERY: 简单 SQL 拉取数据 -> 数据分析 -> 验证 -> 响应
"""
from typing import Literal, Dict, Any
import os
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from state import AgentState, IntentType
from vector_store import get_vector_store
from agents.intent_classifier import create_intent_classifier
from agents.ambiguity_checker import create_ambiguity_checker
from agents.query_planner import create_query_planner
from agents.context_assembler import create_context_assembler
from agents.sql_generator import create_sql_generator
from agents.sql_executor import create_sql_executor
from agents.sql_corrector import create_sql_corrector
from agents.response_generator import create_response_generator
from agents.question_suggester import create_question_suggester
from agents.data_analyzer import create_data_analyzer
from agents.verifier import create_verifier
from agents.python_executor import create_python_executor
from prompts.sql_rules import DatabaseType


# 临时文件目录 (用于清理)
TEMP_DIR = os.path.join(os.path.dirname(__file__), "temp")


def create_graph(llm_client, embedding_client=None, db_connection=None, sql_model_client=None, 
                 domain_config=None, database_type=DatabaseType.MYSQL, max_correction_attempts=3):
    """
    创建 LangGraph 工作流
    
    Args:
        llm_client: 用于意图分类和歧义检测的 LLM 客户端
        embedding_client: 用于向量检索的 Embedding 客户端
        db_connection: 数据库连接
        sql_model_client: SQL 生成模型客户端（微调模型）
        domain_config: 域配置（默认使用 EducationDomain）
        database_type: 数据库类型（用于 SQL 规则）
        max_correction_attempts: 最大 SQL 纠错次数
    
    Returns:
        编译后的 Graph
    """
    
    # 初始化域配置和提示词构建器
    from prompts import EducationDomain, PromptBuilder
    if domain_config is None:
        domain_config = EducationDomain()
    prompt_builder = PromptBuilder(domain_config)
    
    # 创建各个节点（传入 prompt_builder）
    intent_classifier = create_intent_classifier(llm_client, prompt_builder)
    ambiguity_checker = create_ambiguity_checker(llm_client, prompt_builder)
    query_planner = create_query_planner(llm_client)
    context_assembler = create_context_assembler(prompt_builder)
    sql_generator = create_sql_generator(sql_model_client or llm_client)
    sql_executor = create_sql_executor(db_connection)
    sql_corrector = create_sql_corrector(llm_client, database_type)
    response_generator = create_response_generator(llm_client)
    question_suggester = create_question_suggester(llm_client)
    
    # 新增: 指标查询专用节点
    data_analyzer = create_data_analyzer(llm_client)
    python_executor = create_python_executor()  # 代码执行节点
    verifier = create_verifier(llm_client)
    
    # 定义澄清返回节点
    def clarification_return_node(state: AgentState) -> Dict[str, Any]:
        """澄清返回节点 - 将澄清问题作为最终回复"""
        clarification_question = state.get("clarification_question", "请提供更多信息")
        return {
            "final_response": clarification_question,
            "ambiguity_detected": True,
            "clarification_question": clarification_question,
            "current_node": "clarification_return"
        }
    
    # 定义清理节点
    def cleanup_node(state: AgentState) -> Dict[str, Any]:
        """清理临时文件"""
        data_file_path = state.get("data_file_path")
        
        if data_file_path and os.path.exists(data_file_path):
            try:
                os.remove(data_file_path)
                print(f"DEBUG: 已清理临时文件: {data_file_path}")
            except Exception as e:
                print(f"DEBUG: 清理临时文件失败: {e}")
        
        return {"current_node": "cleanup"}
    
    # 定义初始化节点 (设置默认值)
    def init_node(state: AgentState) -> Dict[str, Any]:
        """初始化状态默认值"""
        return {
            "correction_count": 0,
            "verification_count": 0,
            "max_correction_attempts": max_correction_attempts,
            "max_verification_attempts": 2,
            "current_node": "init"
        }
    
    # 创建 StateGraph
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("init", init_node)
    workflow.add_node("intent_classifier", intent_classifier)
    workflow.add_node("ambiguity_checker", ambiguity_checker)
    workflow.add_node("query_planner", query_planner)
    workflow.add_node("clarification_return", clarification_return_node)
    workflow.add_node("context_assembler", context_assembler)
    workflow.add_node("sql_generator", sql_generator)
    workflow.add_node("sql_executor", sql_executor)
    workflow.add_node("sql_corrector", sql_corrector)
    workflow.add_node("data_analyzer", data_analyzer)     # 代码生成
    workflow.add_node("python_executor", python_executor) # 代码执行
    workflow.add_node("verifier", verifier)               # 验证
    workflow.add_node("response_generator", response_generator)
    workflow.add_node("question_suggester", question_suggester)
    workflow.add_node("cleanup", cleanup_node)         # 新增
    
    # 设置入口点
    workflow.set_entry_point("init")
    
    # init -> intent_classifier
    workflow.add_edge("init", "intent_classifier")
    
    # 意图分类后的条件路由
    def route_after_intent(state: AgentState) -> Literal["response_generator", "ambiguity_checker", "query_planner"]:
        intent_type = state.get("intent_type", IntentType.CHITCHAT)
        
        if intent_type == IntentType.CHITCHAT:
            return "response_generator"
        elif intent_type == IntentType.METRIC_DEFINITION:
            return "response_generator"
        elif intent_type == IntentType.VALUE_QUERY:
            return "query_planner"  # 数值查询：直接进入规划器
        else:  # METRIC_QUERY
            return "ambiguity_checker"  # 可能需要澄清聚合方式
    
    workflow.add_conditional_edges(
        "intent_classifier",
        route_after_intent,
        {
            "response_generator": "response_generator",
            "ambiguity_checker": "ambiguity_checker",
            "query_planner": "query_planner"
        }
    )
    
    # 歧义检测后的条件路由
    def route_after_ambiguity(state: AgentState) -> Literal["clarification_return", "query_planner"]:
        ambiguity_detected = state.get("ambiguity_detected", False)
        if ambiguity_detected:
            return "clarification_return"
        return "query_planner"
    
    workflow.add_conditional_edges(
        "ambiguity_checker",
        route_after_ambiguity,
        {
            "clarification_return": "clarification_return",
            "query_planner": "query_planner"
        }
    )
    
    # QueryPlanner -> ContextAssembler
    workflow.add_edge("query_planner", "context_assembler")
    
    # ContextAssembler 后的条件路由 (根据意图类型分流)
    def route_after_context_assembler(state: AgentState) -> Literal["sql_generator", "data_analyzer"]:
        """
        Context Assembler 后路由:
        - METRIC_QUERY -> data_analyzer (Code-Based 模式, 跳过 SQL 生成/执行)
        - VALUE_QUERY -> sql_generator (传统 SQL 执行模式)
        """
        intent_type = state.get("intent_type")
        is_metric_query = (intent_type == IntentType.METRIC_QUERY or 
                          intent_type == "metric_query")
        
        if is_metric_query:
            return "data_analyzer"
        return "sql_generator"
    
    workflow.add_conditional_edges(
        "context_assembler",
        route_after_context_assembler,
        {
            "sql_generator": "sql_generator",
            "data_analyzer": "data_analyzer"
        }
    )
    
    # SQLGenerator -> SQLExecutor (仅 VALUE_QUERY 会走这条路)
    workflow.add_edge("sql_generator", "sql_executor")
    
    # SQL 执行后的条件路由 (仅用于 VALUE_QUERY)
    def route_after_sql_execution(state: AgentState) -> Literal["sql_corrector", "response_generator"]:
        """
        SQL 执行后路由 (仅 VALUE_QUERY 会走这里):
        - 有错误 -> sql_corrector
        - 成功 -> response_generator
        
        注意: METRIC_QUERY 已在 context_assembler 后直接走 data_analyzer，
              不会经过 sql_executor。
        """
        execution_error = state.get("execution_error")
        execution_result = state.get("execution_result")
        correction_count = state.get("correction_count", 0)
        max_limit = state.get("max_correction_attempts", 3)
        
        # 场景 1: 数据库硬报错
        if execution_error:
            if correction_count < max_limit:
                return "sql_corrector"
            # 达到上限, 直接响应错误
            return "response_generator"
        
        # 场景 2: 空结果 - 尝试纠错
        if execution_result is not None and len(execution_result) == 0:
            if correction_count < 2:
                return "sql_corrector"
        
        # 场景 3: 正常结束
        return "response_generator"
    
    workflow.add_conditional_edges(
        "sql_executor",
        route_after_sql_execution,
        {
            "sql_corrector": "sql_corrector",
            "response_generator": "response_generator"
        }
    )
    
    # SQL 纠错后重新执行
    workflow.add_edge("sql_corrector", "sql_executor")
    
    # 数据分析 -> 代码执行 -> 验证
    workflow.add_edge("data_analyzer", "python_executor")
    workflow.add_edge("python_executor", "verifier")
    
    # 验证后的条件路由
    def route_after_verification(state: AgentState) -> Literal["data_analyzer", "response_generator"]:
        """
        验证后路由:
        - 验证通过 -> response_generator
        - 验证失败且未达上限 -> data_analyzer (重新生成分析代码)
        - 验证失败且达上限 -> response_generator (带错误信息)
        """
        verification_passed = state.get("verification_passed", False)
        verification_count = state.get("verification_count", 0)
        max_attempts = state.get("max_verification_attempts", 2)
        
        if verification_passed:
            return "response_generator"
        
        # 未通过验证
        if verification_count < max_attempts:
            return "data_analyzer"
        
        return "response_generator"
    
    workflow.add_conditional_edges(
        "verifier",
        route_after_verification,
        {
            "data_analyzer": "data_analyzer",
            "response_generator": "response_generator"
        }
    )
    
    # 响应生成后的路由 (推荐问题)
    def route_after_response(state: AgentState) -> str:
        """
        根据配置决定是否生成推荐问题
        
        注意: Code-Based 模式下 METRIC_QUERY 不再产生 CSV，无需 cleanup
        """
        enable = state.get("enable_suggestions", False)
        
        if enable:
            return "question_suggester"
        return "end"

    workflow.add_conditional_edges(
        "response_generator",
        route_after_response,
        {
            "question_suggester": "question_suggester",
            "end": END
        }
    )
    
    # 推荐问题后直接结束
    workflow.add_edge("question_suggester", END)
    
    # 清理后结束
    workflow.add_edge("cleanup", END)
    
    # 澄清返回直接结束
    workflow.add_edge("clarification_return", END)
    
    # 编译（增加 Checkpointer 支持多轮对话记忆）
    memory = MemorySaver()
    app = workflow.compile(checkpointer=memory)
    
    return app


def process_clarification(app, state: AgentState, user_response: str) -> AgentState:
    """
    处理用户的澄清回复
    
    Args:
        app: 编译后的 Graph
        state: 当前状态
        user_response: 用户的澄清回复
    
    Returns:
        更新后的状态
    """
    new_state = dict(state)
    new_state["clarification_response"] = user_response
    new_state["messages"] = state.get("messages", []) + [
        {"role": "assistant", "content": state.get("clarification_question", "")},
        {"role": "user", "content": user_response}
    ]
    
    result = app.invoke(new_state)
    return result


def cleanup_temp_directory():
    """
    启动时清理临时目录中的残留文件
    建议在 main.py 启动时调用
    """
    if os.path.exists(TEMP_DIR):
        for filename in os.listdir(TEMP_DIR):
            if filename.startswith("query_") and filename.endswith(".csv"):
                filepath = os.path.join(TEMP_DIR, filename)
                try:
                    os.remove(filepath)
                    print(f"清理残留临时文件: {filepath}")
                except Exception as e:
                    print(f"清理失败: {filepath}, 错误: {e}")
