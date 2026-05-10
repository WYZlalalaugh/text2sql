"""
LangGraph 图编排 - 定义智能体工作流

支持两种主要流程:
1. VALUE_QUERY: 直接 SQL 生成 -> 执行 -> 响应
2. METRIC_QUERY: 简单 SQL 拉取数据 -> 数据分析 -> 验证 -> 响应
"""
from typing import Literal, Dict, Any
import os
import time
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from state import AgentState, IntentType
from agents.intent_classifier import create_intent_classifier
from agents.ambiguity_checker import create_ambiguity_checker
from agents.query_planner import create_query_planner
from agents.context_assembler import create_context_assembler
from agents.sql_generator import create_sql_generator
from agents.sql_executor import create_sql_executor
from agents.sql_corrector import create_sql_corrector
from agents.response_generator import create_response_generator
from agents.question_suggester import create_question_suggester
from agents.verifier import create_verifier
from agents.python_executor import create_python_executor
from agents.metric_loop_planner import create_metric_loop_planner
from agents.metric_sql_generator import create_metric_sql_generator
from agents.metric_executor import create_metric_executor
from agents.metric_observer import create_metric_observer
from prompts.sql_rules import DatabaseType


# 临时文件目录 (用于清理)
TEMP_DIR = os.path.join(os.path.dirname(__file__), "temp")


class MetricDBConnectionManager:
    """
    Metric 循环专用数据库连接管理器
    
    在 metric 循环中共享同一个连接，避免临时表消失问题。
    循环结束时统一清理临时表和连接。
    """
    
    def __init__(self, db_config):
        self.db_config = db_config
        self._connection = None
        self._tables_created = []
    
    def get_connection(self):
        """获取或创建共享连接"""
        if self._connection is None:
            import mysql.connector
            import logging
            
            # 验证数据库配置完整性
            if self.db_config is None:
                raise ValueError("数据库配置缺失: db_config 为 None")
            
            required_attrs = ['host', 'port', 'user', 'password', 'database']
            for attr in required_attrs:
                if not hasattr(self.db_config, attr) or getattr(self.db_config, attr) is None:
                    raise ValueError(f"数据库配置缺失: db_config.{attr} 未配置")
            
            self._connection = mysql.connector.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                charset=getattr(self.db_config, 'charset', 'utf8mb4'),
            )
        return self._connection
    
    def register_table(self, table_name: str):
        """注册创建的表名，用于后续清理"""
        if table_name not in self._tables_created:
            self._tables_created.append(table_name)
    
    def cleanup(self):
        """清理所有临时表并关闭连接"""
        if self._connection is None:
            return
        
        errors = []
        try:
            cursor = self._connection.cursor()
            try:
                for table_name in self._tables_created:
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                    except Exception as e:
                        errors.append(f"清理表 {table_name} 失败: {e}")
                self._connection.commit()
            except Exception as e:
                errors.append(f"清理过程出错: {e}")
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        finally:
            try:
                self._connection.close()
            except Exception as e:
                errors.append(f"关闭连接失败: {e}")
            finally:
                self._connection = None
                self._tables_created = []
                
        if errors:
            import logging
            logging.getLogger(__name__).warning(f"MetricDBConnectionManager 清理警告: {'; '.join(errors)}")


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
    
    python_executor = create_python_executor()  # 代码执行节点
    verifier = create_verifier(llm_client)
    
    # 创建 Metric DB 连接管理器（用于共享连接）
    from config import config as app_config
    metric_db_manager = MetricDBConnectionManager(app_config.database)
    
    metric_loop_planner = create_metric_loop_planner(llm_client)
    metric_sql_generator = create_metric_sql_generator(llm_client)
    metric_executor = create_metric_executor(metric_db_manager)
    metric_observer = create_metric_observer(llm_client)
    
    # 定义 Metric 清理节点（循环结束时清理连接和临时表）
    def metric_cleanup_node(state: AgentState) -> Dict[str, Any]:
        """Metric 循环清理节点 - 清理共享连接和临时表"""
        try:
            metric_db_manager.cleanup()
        except Exception as e:
            print(f"DEBUG: Metric cleanup failed: {e}")
        return {"current_node": "metric_cleanup"}
    
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
        """初始化状态默认值 - 重置所有请求级字段避免状态污染"""
        
        # 获取会话级持久字段（需要保留的）
        workspace_id = state.get("workspace_id")
        session_id = state.get("session_id")
        user_id = state.get("user_id")
        messages = state.get("messages", [])
        incoming_clarification = state.get("clarification_response")
        incoming_clarification_count = state.get("clarification_count", 0)
        preserve_clarification = bool(incoming_clarification)

        if isinstance(incoming_clarification_count, int):
            clarification_count = incoming_clarification_count if preserve_clarification else 0
        else:
            clarification_count = 0
        
        # 返回全新状态，只保留会话级字段
        return {
            # === 会话级持久字段（跨请求保留）===
            "workspace_id": workspace_id,
            "session_id": session_id,
            "user_id": user_id,
            "messages": messages,
            
            # === 用户输入（新请求）===
            "user_query": state.get("user_query", ""),
            "clarification_response": incoming_clarification if preserve_clarification else None,
            
            # === 意图和分类（新请求）===
            "intent_type": None,
            "intent_confidence": 0.0,
            
            # === 歧义检测（新请求）===
            "ambiguity_detected": False,
            "ambiguity_details": [],
            "clarification_question": None,
            "refined_intent": None,
            "clarification_count": clarification_count,
            
            # === 规划相关（新请求）===
            "query_plan": {},
            "reasoning_plan": "",
            "selected_metrics": [],
            "target_fields": [],
            "planning_error": None,
            "metrics_context": None,
            
            # === 上下文组装（新请求）===
            "schema_context": None,
            "assembled_prompt": None,
            "context_assembled": False,
            
            # === Metric 循环相关（新请求）===
            "metric_plan_nodes": [],
            "current_step_id": None,
            "step_results": {},
            "step_status": None,
            "step_status_map": {},
            "planner_observations": [],
            "execution_history": [],
            "materialized_artifacts": {},
            "retry_counters": {},
            "loop_iteration": 0,
            "loop_status": "planning",
            "loop_decision": None,
            
            # === SQL 生成和执行（新请求）===
            "generated_sql": "",
            "execution_result": None,
            "execution_error": None,
            
            # === 纠错和验证（新请求）===
            "correction_count": 0,
            "verification_count": 0,
            "max_correction_attempts": max_correction_attempts,
            "max_verification_attempts": 2,
            "analysis_result": None,
            "analysis_error": None,
            "analysis_code": None,
            
            # === 最终响应（新请求）===
            "final_response": None,
            "recommended_questions": [],
            
            # === 遗留兼容字段（新请求）===
            "execution_path": state.get("execution_path"),
            "legacy_fallback_triggered": False,
            "legacy_fallback_reason": None,
            "legacy_fallback_count": 0,
            
            # === 配置和开关（新请求）===
            "enable_suggestions": state.get("enable_suggestions", False),
            "enable_streaming": state.get("enable_streaming", False),
            "chart_config": state.get("chart_config"),
            
            # === 元数据 ===
            "current_node": "init",
            "start_time": int(time.time() * 1000),
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
    workflow.add_node("python_executor", python_executor) # 代码执行
    workflow.add_node("verifier", verifier)               # 验证
    workflow.add_node("metric_loop_planner", metric_loop_planner)
    workflow.add_node("metric_sql_generator", metric_sql_generator)
    workflow.add_node("metric_executor", metric_executor)
    workflow.add_node("metric_observer", metric_observer)
    workflow.add_node("metric_cleanup", metric_cleanup_node)  # 新增清理节点
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
    def route_after_ambiguity(state: AgentState) -> Literal["clarification_return", "query_planner", "metric_loop_planner"]:
        ambiguity_detected = state.get("ambiguity_detected", False)
        if ambiguity_detected:
            return "clarification_return"

        intent_type = state.get("intent_type")
        is_metric_query = (intent_type == IntentType.METRIC_QUERY or
                          intent_type == "metric_query")
        # 关键调整: METRIC_QUERY 不再经过 query_planner，直接进入 metric_loop_planner
        if is_metric_query:
            return "metric_loop_planner"

        return "query_planner"
    
    workflow.add_conditional_edges(
        "ambiguity_checker",
        route_after_ambiguity,
        {
            "clarification_return": "clarification_return",
            "query_planner": "query_planner",
            "metric_loop_planner": "metric_loop_planner",
        }
    )
    
    # QueryPlanner -> 条件路由 (规划失败时短路到 response_generator)
    def route_after_planner(state: AgentState) -> Literal["context_assembler", "response_generator"]:
        """
        Query Planner 后路由:
        - 规划失败 -> response_generator (错误响应)
        - VALUE_QUERY -> context_assembler (传统路径)

        关键约束: METRIC_QUERY 不会进入该节点（已在 ambiguity_checker 后直达 metric_loop_planner）
        """
        planning_error = state.get("planning_error")
        if planning_error:
            return "response_generator"

        return "context_assembler"
    
    workflow.add_conditional_edges(
        "query_planner",
        route_after_planner,
        {
            "context_assembler": "context_assembler",
            "response_generator": "response_generator"
        }
    )
    
    # ContextAssembler 仅用于 VALUE_QUERY，固定进入 sql_generator
    workflow.add_edge("context_assembler", "sql_generator")

    def route_metric_loop(state: AgentState) -> Literal["metric_sql_generator", "metric_cleanup"]:
        loop_status = state.get("loop_status")
        if loop_status in ["completed", "failed"]:
            return "metric_cleanup"  # 先清理再返回响应
        return "metric_sql_generator"

    workflow.add_conditional_edges(
        "metric_loop_planner",
        route_metric_loop,
        {"metric_sql_generator": "metric_sql_generator", "metric_cleanup": "metric_cleanup"}
    )
    
    # Metric SQL Generator 后的条件路由（关键修复：SQL生成错误时不进入executor）
    def route_after_metric_sql_generator(state: AgentState) -> Literal["metric_executor", "metric_observer"]:
        """
        Metric SQL Generator 后路由:
        - SQL生成成功 -> metric_executor（执行SQL）
        - SQL生成失败 -> metric_observer（记录错误并触发重试）
        
        关键修复：避免生成错误覆盖执行错误，保持错误链完整
        """
        execution_error = state.get("execution_error")
        generated_sql = state.get("generated_sql")
        
        # 如果SQL生成返回错误，或没有生成SQL，直接进入observer记录失败
        if execution_error or not generated_sql:
            return "metric_observer"
        
        return "metric_executor"
    
    workflow.add_conditional_edges(
        "metric_sql_generator",
        route_after_metric_sql_generator,
        {"metric_executor": "metric_executor", "metric_observer": "metric_observer"}
    )
    workflow.add_edge("metric_executor", "metric_observer")
    
    # Metric Observer 后的条件路由（Oracle 建议：根据 step_status 强制门控）
    def route_after_metric_observer(state: AgentState) -> Literal["metric_loop_planner", "metric_cleanup"]:
        """
        Metric Observer 后路由（关键！Oracle 建议的状态门控）:
        - step_status == "succeeded": 继续到 planner 进行下一步规划
        - step_status in ["failed_execution", "failed_validation"]: 返回 planner 进行重试/调整
        - loop_status in ["completed", "failed"]: 到 cleanup 结束循环
        
        注意：失败的步骤绝不会继续到下游，确保状态一致性。
        """
        loop_status = state.get("loop_status")
        step_status = state.get("step_status")
        
        # 循环结束状态优先
        if loop_status in ["completed", "failed"]:
            return "metric_cleanup"
        
        # 步骤成功或任何失败都返回 planner（由 planner 决定重试或继续）
        # succeeded -> planner 会继续下一步
        # failed_execution/failed_validation -> planner 会重试当前步骤
        return "metric_loop_planner"
    
    workflow.add_conditional_edges(
        "metric_observer",
        route_after_metric_observer,
        {"metric_loop_planner": "metric_loop_planner", "metric_cleanup": "metric_cleanup"}
    )
    
    workflow.add_edge("metric_cleanup", "response_generator")  # 清理后到响应生成

    # SQLGenerator -> SQLExecutor (仅 VALUE_QUERY 会走这条路)
    workflow.add_edge("sql_generator", "sql_executor")
    
    # SQL 执行后的条件路由 (仅用于 VALUE_QUERY)
    def route_after_sql_execution(state: AgentState) -> Literal["sql_corrector", "response_generator"]:
        """
        SQL 执行后路由 (仅 VALUE_QUERY 会走这里):
        - 有错误 -> sql_corrector
        - 成功 -> response_generator
        
        注意: METRIC_QUERY 在 ambiguity_checker 后直接进入 metric_loop_planner，
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
        if isinstance(execution_result, list) and len(execution_result) == 0:
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
    
    workflow.add_edge("python_executor", "verifier")
    
    # 验证后的条件路由
    def route_after_verification(state: AgentState) -> Literal["response_generator"]:
        """
        验证后路由:
        - 验证通过 -> response_generator
        - 验证失败且未达上限 -> response_generator（当前仅保留最终输出）
        - 验证失败且达上限 -> response_generator (带错误信息)
        """
        return "response_generator"
    
    workflow.add_conditional_edges(
        "verifier",
        route_after_verification,
        {
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


def process_clarification(
    app,
    state: AgentState,
    user_response: str,
    config: dict[str, object] | None = None,
) -> AgentState:
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
    
    result = app.invoke(new_state, config=config)
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
