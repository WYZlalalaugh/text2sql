"""
LangGraph 图编排 - 定义智能体工作流
"""
from typing import Literal, Dict, Any
from langgraph.graph import StateGraph, END

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
from prompts.sql_rules import DatabaseType


def create_graph(llm_client, embedding_client=None, db_connection=None, sql_model_client=None, 
                 domain_config=None, database_type=DatabaseType.MYSQL, max_correction_attempts=2):
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
    
    # 初始化向量存储
    vector_store = get_vector_store(embedding_client)
    
    # 创建各个节点（传入 prompt_builder）
    intent_classifier = create_intent_classifier(llm_client, prompt_builder)
    ambiguity_checker = create_ambiguity_checker(llm_client, prompt_builder)
    query_planner = create_query_planner(llm_client)  # 新增：查询规划器
    context_assembler = create_context_assembler(prompt_builder)
    sql_generator = create_sql_generator(sql_model_client or llm_client)  # 如果没有微调模型，使用普通 LLM
    sql_executor = create_sql_executor(db_connection)
    sql_corrector = create_sql_corrector(llm_client, database_type)
    response_generator = create_response_generator(llm_client)
    
    # 定义向量检索节点
    def vector_search_node(state: AgentState) -> Dict[str, Any]:
        """向量检索节点"""
        user_query = state.get("user_query", "")
        matched_metrics = vector_store.search(user_query)
        return {
            "matched_metrics": matched_metrics,
            "current_node": "vector_search",
            "correction_count": 0,
            "max_correction_attempts": max_correction_attempts
        }
    
    # 定义澄清返回节点
    def clarification_return_node(state: AgentState) -> Dict[str, Any]:
        """澄清返回节点 - 将澄清问题作为最终回复"""
        clarification_question = state.get("clarification_question", "请提供更多信息")
        return {
            "final_response": clarification_question,
            "ambiguity_detected": True,  # 关键：标记需要澄清
            "clarification_question": clarification_question,  # 保留原始问题
            "current_node": "clarification_return"
        }
    
    # 创建 StateGraph
    workflow = StateGraph(AgentState)
    
    # 添加节点
    workflow.add_node("vector_search", vector_search_node)
    workflow.add_node("intent_classifier", intent_classifier)
    workflow.add_node("ambiguity_checker", ambiguity_checker)
    workflow.add_node("query_planner", query_planner)  # 新增
    workflow.add_node("clarification_return", clarification_return_node)
    workflow.add_node("context_assembler", context_assembler)
    workflow.add_node("sql_generator", sql_generator)
    workflow.add_node("sql_executor", sql_executor)
    workflow.add_node("response_generator", response_generator)
    
    # 设置入口点 - 直接进入意图分类，跳过向量检索
    workflow.set_entry_point("intent_classifier")
    
    # 定义边
    # workflow.add_edge("vector_search", "intent_classifier") # 已断开
    
    # 意图分类后的条件路由
    def route_after_intent(state: AgentState) -> Literal["response_generator", "ambiguity_checker", "query_planner"]:
        intent_type = state.get("intent_type", IntentType.CHITCHAT)
        
        if intent_type == IntentType.CHITCHAT:
            return "response_generator"
        elif intent_type == IntentType.METRIC_DEFINITION:
            return "response_generator"
        elif intent_type == IntentType.SIMPLE_QUERY:
            return "query_planner"  # 改：简单查询也经过规划器
        else:  # METRIC_QUERY
            return "ambiguity_checker"
    
    workflow.add_conditional_edges(
        "intent_classifier",
        route_after_intent,
        {
            "response_generator": "response_generator",
            "ambiguity_checker": "ambiguity_checker",
            "query_planner": "query_planner"
        }
    )
    
    # 歧义检测后的条件路由 - 现在指向 query_planner
    def route_after_ambiguity(state: AgentState) -> Literal["clarification_return", "query_planner"]:
        ambiguity_detected = state.get("ambiguity_detected", False)
        if ambiguity_detected:
            return "clarification_return"
        return "query_planner"  # 改：歧义检测后进入规划器
    
    workflow.add_conditional_edges(
        "ambiguity_checker",
        route_after_ambiguity,
        {
            "clarification_return": "clarification_return",
            "query_planner": "query_planner"  # 改
        }
    )
    
    # 新增：QueryPlanner -> ContextAssembler
    workflow.add_edge("query_planner", "context_assembler")
    
    # 后续节点的边
    workflow.add_edge("context_assembler", "sql_generator")
    workflow.add_edge("sql_generator", "sql_executor")
    
    # SQL 执行后的条件路由：检查是否需要纠错
    def route_after_sql_execution(state: AgentState) -> Literal["sql_corrector", "response_generator"]:
        execution_error = state.get("execution_error")
        correction_count = state.get("correction_count", 0)
        max_attempts = state.get("max_correction_attempts", max_correction_attempts)
        
        # 如果有错误且未超过最大重试次数，进行纠错
        if execution_error and correction_count < max_attempts:
            return "sql_corrector"
        return "response_generator"
    
    workflow.add_node("sql_corrector", sql_corrector)
    
    workflow.add_conditional_edges(
        "sql_executor",
        route_after_sql_execution,
        {
            "sql_corrector": "sql_corrector",
            "response_generator": "response_generator"
        }
    )
    
    # 纠错后重新执行 SQL
    def increment_correction_count(state: AgentState) -> Dict[str, Any]:
        return {"correction_count": state.get("correction_count", 0) + 1}
    
    workflow.add_edge("sql_corrector", "sql_executor")
    
    # 结束节点
    workflow.add_edge("response_generator", END)
    workflow.add_edge("clarification_return", END)
    
    # 编译
    app = workflow.compile()
    
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
    # 更新状态
    new_state = dict(state)
    new_state["clarification_response"] = user_response
    new_state["messages"] = state.get("messages", []) + [
        {"role": "assistant", "content": state.get("clarification_question", "")},
        {"role": "user", "content": user_response}
    ]
    
    # 从歧义检测节点重新开始
    # 由于 LangGraph 的设计，我们需要重新运行整个图
    # 但这次带上澄清信息
    result = app.invoke(new_state)
    return result
