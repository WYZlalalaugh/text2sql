"""
Text2SQL API 服务 - FastAPI 后端（支持流式响应）
"""
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from typing import Optional, AsyncGenerator, Any
import os
import re
import json
import asyncio
import uuid
import logging
from decimal import Decimal
from datetime import date, datetime

class CustomJSONEncoder(json.JSONEncoder):
    """处理 Decimal 和日期类型的 JSON 编码器"""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        return super().default(o)

from state import AgentState
from config import config
from graph import process_clarification
from runtime_bootstrap import create_runtime_graph  # pyright: ignore[reportMissingImports]
from tools.result_normalizer import normalize_canonical_tabular_result
from tools.auth_utils import create_access_token, decode_access_token
from tools.chat_store import (
    AuthenticatedUser,
    append_chat_message,
    authenticate_user,
    bootstrap_admin_user,
    ensure_chat_schema_initialized,
    ensure_conversation,
    get_conversation,
    get_conversation_messages,
    list_conversations,
    make_message_preview,
    update_conversation_after_message,
)

app = FastAPI(title="Text2SQL Assistant", version="1.0.0")

# CORS 配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注: tools.db_client 会在首次调用 load_data 时自动从 config 读取数据库配置


class ChatRequest(BaseModel):
    message: str
    session_id: str = "default"
    enable_suggestions: bool = False
    workspace_id: Optional[str] = None
    thread_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    need_clarification: bool
    sql: Optional[str] = None
    intent_type: Optional[str] = None


class LoginRequest(BaseModel):
    """登录请求体。"""
    username: str
    password: str


class ConversationMessageItem(BaseModel):
    """单条历史消息结构。"""
    role: str
    content: str = ""
    steps: list[dict[str, Any]] = []
    sql: Optional[str] = None
    pythonCode: Optional[str] = None
    needClarification: bool = False
    clarificationSections: list[Any] = []
    reflection: str = ""
    reasoning: str = ""
    chartReasoning: str = ""
    chartSpec: Optional[dict[str, Any]] = None
    sqlResult: Any = None
    totalCount: Optional[int] = None
    isTruncated: bool = False


class SaveConversationMessagesRequest(BaseModel):
    """保存会话历史请求体。"""
    session_id: Optional[str] = None
    workspace_id: Optional[str] = None
    title: Optional[str] = None
    enable_suggestions: bool = False
    suggested_questions: list[str] = []
    messages: list[ConversationMessageItem]


# 会话存储
DEFAULT_WORKSPACE_ID = "default"
sessions = {}
bearer_scheme = HTTPBearer(auto_error=False)
chat_schema_ready = {"ready": False}


def _normalize_workspace_id(workspace_id: Optional[str]) -> str:
    return workspace_id or DEFAULT_WORKSPACE_ID


def _build_session_key(session_id: str, workspace_id: Optional[str] = None) -> str:
    normalized_workspace_id = _normalize_workspace_id(workspace_id)
    return f"{normalized_workspace_id}:{session_id}"


def _serialize_intent_type(intent_type: Any) -> str:
    """将意图类型统一序列化为稳定的字符串。"""
    if intent_type is None:
        return ""
    value = getattr(intent_type, "value", None)
    if isinstance(value, str):
        return value
    return str(intent_type)


def _ensure_history_services_ready() -> None:
    """初始化历史记录依赖，只允许作用于当前项目数据库。"""
    if chat_schema_ready["ready"]:
        return

    ensure_chat_schema_initialized()
    bootstrap_admin_user(
        config.auth.bootstrap_admin_username,
        config.auth.bootstrap_admin_password,
    )
    chat_schema_ready["ready"] = True


def _get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> AuthenticatedUser:
    """解析并校验登录用户。"""
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="缺少登录凭证")

    try:
        payload = decode_access_token(credentials.credentials, config.auth.jwt_secret)
        return AuthenticatedUser(
            id=int(payload["uid"]),
            username=str(payload["username"]),
            status=str(payload.get("status") or "active"),
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="登录凭证无效") from exc


def _get_owned_conversation_or_404(user_id: int, conversation_id: str) -> dict[str, Any]:
    """读取当前用户拥有的会话，不存在时返回 404。"""
    try:
        return get_conversation(user_id=user_id, conversation_id=conversation_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="会话不存在") from exc


def get_or_create_session(session_id: str, workspace_id: Optional[str] = None):
    """获取或创建会话"""
    normalized_workspace_id = _normalize_workspace_id(workspace_id)
    session_key = _build_session_key(session_id, normalized_workspace_id)

    if session_key not in sessions:
        graph, _, _ = create_runtime_graph(
            enable_embedding_in_graph=True,
        )

        sessions[session_key] = {
            "session_id": session_id,
            "workspace_id": normalized_workspace_id,
            "session_key": session_key,
            "graph": graph,
            "state": None,
            "waiting_for_clarification": False,
            "waiting_for_plan_review": False
        }

    return sessions[session_key]


# 节点名称映射（前端步骤展示）
NODE_DISPLAY_NAMES = {
    "vector_search": "向量检索",
    "intent_classifier": "意图识别",
    "ambiguity_checker": "歧义检测",
    "query_planner": "查询规划",
    "context_assembler": "上下文组装",
    "sql_generator": "SQL 生成",
    "sql_executor": "SQL 执行",
    "sql_corrector": "SQL 修正",
    "metric_loop_planner": "指标循环规划",
    "metric_sql_generator": "指标 SQL 生成",
    "metric_executor": "指标执行",
    "metric_observer": "指标观察",
    "metric_cleanup": "指标清理",
    "plan_review_handler": "计划审核",
    "data_analyzer": "代码生成",
    "python_executor": "代码执行",
    "verifier": "结果验证",
    "response_generator": "回答生成",
}


def _build_step_data(
    node_name: str,
    node_output: dict[str, Any],
    current_step: int,
    *,
    accumulated_state: AgentState,
    use_accumulated_query_plan: bool = False,
) -> dict[str, Any]:
    """构建前端使用的稳定步骤事件数据。"""
    step_data = {
        "type": "step",
        "step": current_step,
        "node": node_name,
        "title": NODE_DISPLAY_NAMES[node_name],
        "status": "complete",
        "message": f"{NODE_DISPLAY_NAMES[node_name]}完成",
    }

    if node_name == "intent_classifier" and "intent_type" in node_output:
        intent_str = _serialize_intent_type(node_output["intent_type"])
        step_data["detail"] = f"识别意图: {intent_str}"
    elif node_name == "query_planner":
        plan = node_output.get("query_plan", {})
        reasoning = node_output.get("reasoning_plan", "")
        if use_accumulated_query_plan:
            plan = node_output.get("query_plan") or accumulated_state.get("query_plan", {})
            reasoning = node_output.get("reasoning_plan") or accumulated_state.get("reasoning_plan", "")

        selected = plan.get("selected_metrics", []) if isinstance(plan, dict) else []
        calc = plan.get("calculation_type", "") if isinstance(plan, dict) else ""
        if isinstance(selected, list) and selected:
            step_data["detail"] = f"筛选指标: {', '.join(selected[:2])}{'...' if len(selected) > 2 else ''}"
        elif calc:
            step_data["detail"] = f"计算类型: {calc}"
        else:
            step_data["detail"] = "正在规划查询路径..."
        if reasoning:
            step_data["reasoning"] = reasoning
    elif node_name == "sql_generator" and "generated_sql" in node_output:
        step_data["detail"] = "SQL 语句已生成"
        step_data["sql"] = node_output.get("generated_sql", "")
    elif node_name == "sql_executor":
        if "execution_result" in node_output:
            results = node_output.get("execution_result", [])
            if isinstance(results, list) and results:
                step_data["detail"] = f"查询返回 {len(results)} 条结果"
            else:
                step_data["detail"] = "查询未返回结果"
        elif "execution_error" in node_output:
            step_data["detail"] = "执行出错，准备纠错"
    elif node_name == "sql_corrector":
        step_data["detail"] = "AI 正在分析执行结果并修正 SQL..."
        if "sql_reflection" in node_output:
            step_data["reflection"] = node_output.get("sql_reflection", "")
        if "generated_sql" in node_output:
            step_data["sql"] = node_output.get("generated_sql", "")
    elif node_name == "data_analyzer":
        if node_output.get("analysis_code"):
            code = node_output.get("analysis_code", "")
            step_data["detail"] = f"已生成分析代码（{len(code)} 字符）"
            step_data["python_code"] = code
        elif node_output.get("analysis_error"):
            step_data["detail"] = "代码生成遇到问题"
        else:
            step_data["detail"] = "正在生成分析代码..."
    elif node_name == "python_executor":
        if node_output.get("analysis_result"):
            result = node_output.get("analysis_result")
            if isinstance(result, list):
                step_data["detail"] = f"代码执行成功，返回 {len(result)} 条结果"
            else:
                step_data["detail"] = "代码执行成功"
        elif node_output.get("analysis_error"):
            step_data["detail"] = f"执行出错: {str(node_output.get('analysis_error', ''))[:50]}..."
        else:
            step_data["detail"] = "正在执行分析代码..."
    elif node_name == "verifier":
        if node_output.get("verification_passed"):
            step_data["detail"] = "验证通过"
        else:
            feedback = node_output.get("verification_feedback", "")
            step_data["detail"] = f"验证中: {feedback[:50]}..." if feedback else "验证中..."
    # ---------- 指标循环附加字段 ----------
    if node_name in ("metric_loop_planner", "metric_sql_generator", "metric_executor", "metric_observer"):
        step_id = (accumulated_state.get("current_step_id") or "") if accumulated_state else ""
        if step_id:
            step_data["metric_step_id"] = step_id

        step_status_map = accumulated_state.get("step_status_map") or {} if accumulated_state else {}
        if step_id and step_id in step_status_map:
            step_data["metric_step_status"] = step_status_map[step_id]

    if node_name == "metric_loop_planner":
        plan_nodes = (node_output.get("metric_plan_nodes") or []) if isinstance(node_output, dict) else []
        if not plan_nodes and accumulated_state:
            plan_nodes = accumulated_state.get("metric_plan_nodes") or []
        if plan_nodes:
            step_data["metric_plan"] = plan_nodes
        decision = node_output.get("loop_decision", {})
        if isinstance(decision, dict):
            decision_type = str(decision.get("decision", "") or "")
            reason = str(decision.get("reason", "") or "")
            next_step_id = str(decision.get("next_step_id", "") or "")
            if next_step_id:
                step_data["detail"] = f"决策: {decision_type}，下一步 {next_step_id}"
            elif reason:
                step_data["detail"] = f"决策: {decision_type}，{reason[:60]}"
            elif decision_type:
                step_data["detail"] = f"决策: {decision_type}"
            else:
                step_data["detail"] = "正在更新指标循环计划..."
        else:
            step_data["detail"] = "正在更新指标循环计划..."
    elif node_name == "metric_sql_generator":
        if node_output.get("execution_error"):
            err = str(node_output.get("execution_error", ""))
            step_data["detail"] = f"SQL 生成失败: {err[:60]}"
        elif node_output.get("generated_sql"):
            step_data["detail"] = "指标 SQL 已生成"
            step_data["sql"] = node_output.get("generated_sql", "")
            # 把 SQL 存入 metric_step_sqls 便于前端按 step_id 索引
            sid = step_data.get("metric_step_id", "")
            if sid:
                step_data["metric_step_sql"] = node_output.get("generated_sql", "")
        else:
            step_data["detail"] = "正在生成指标 SQL..."
    elif node_name == "metric_executor":
        if node_output.get("execution_error"):
            err = str(node_output.get("execution_error", ""))
            step_data["detail"] = f"执行失败: {err[:60]}"
            sid = step_data.get("metric_step_id", "")
            if sid:
                step_data["metric_step_error"] = err
        elif isinstance(node_output.get("execution_result"), list):
            rows = node_output.get("execution_result") or []
            step_data["detail"] = f"执行完成，返回 {len(rows)} 条结果"
            # 最终步骤也附带结果摘要供前端展示
            sid = step_data.get("metric_step_id", "")
            if sid:
                step_data["metric_step_result"] = {"row_count": len(rows)}
        elif isinstance(node_output.get("execution_result"), dict):
            exec_result = node_output.get("execution_result") or {}
            row_count = exec_result.get("row_count")
            table_name = exec_result.get("output_table")
            if row_count is not None and table_name:
                step_data["detail"] = f"执行完成，写入中间表 {table_name}（{row_count} 行）"
            elif row_count is not None:
                step_data["detail"] = f"执行完成，{row_count} 行"
            else:
                step_data["detail"] = "指标 SQL 执行完成"
            # 附带结果摘要供前端展示
            sid = step_data.get("metric_step_id", "")
            if sid:
                step_data["metric_step_result"] = {
                    "row_count": row_count,
                    "output_table": table_name,
                }
        else:
            step_data["detail"] = "正在执行指标 SQL..."
    elif node_name == "metric_observer":
        step_status = str(node_output.get("step_status", "") or "")
        step_data["detail"] = f"当前步骤状态: {step_status}" if step_status else "正在评估执行结果..."
    elif node_name == "metric_cleanup":
        step_data["detail"] = "已完成临时表与连接清理"

    return step_data


async def _stream_graph_events(
    graph,
    initial_state: AgentState,
    session: dict[str, Any],
    *,
    start_message: str,
    error_message_prefix: Optional[str] = None,
    use_accumulated_query_plan: bool = False,
    include_sql_reflection: bool = False,
    update_waiting_for_clarification: bool = False,
) -> AsyncGenerator[str, None]:
    """复用的 Graph SSE 流式输出逻辑"""
    try:
        yield f"data: {json.dumps({'type': 'start', 'message': start_message}, ensure_ascii=False)}\n\n"
        await asyncio.sleep(0.1)

        current_step = 1
        accumulated_state = initial_state.copy()

        thread_id = session.get("current_thread_id") or session.get("session_key", "default_thread")
        config_dict = {
            "recursion_limit": 50,
            "configurable": {"thread_id": thread_id}
        }

        for event in graph.stream(initial_state, config=config_dict):
            for node_name, node_output in event.items():
                if node_output and isinstance(node_output, dict):
                    accumulated_state.update(node_output)

                if node_name not in NODE_DISPLAY_NAMES:
                    continue

                step_data = _build_step_data(
                    node_name,
                    node_output or {},
                    current_step,
                    accumulated_state=accumulated_state,
                    use_accumulated_query_plan=use_accumulated_query_plan,
                )

                yield f"data: {json.dumps(step_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
                current_step += 1
                await asyncio.sleep(0.1)

        # 检查是否有 interrupt（计划审核暂停）
        interrupt_data = None
        try:
            snapshot = graph.get_state(config_dict)
            if snapshot and snapshot.tasks:
                for task in snapshot.tasks:
                    if task.interrupts:
                        interrupt_value = task.interrupts[0].value
                        if isinstance(interrupt_value, dict) and interrupt_value.get("plan_nodes"):
                            interrupt_data = interrupt_value
                            break
        except Exception as e:
            print(f"DEBUG: get_state check failed: {e}")

        if interrupt_data:
            plan_review_event = {
                "type": "plan_review",
                "plan_nodes": interrupt_data.get("plan_nodes", []),
                "message": "请审核以下查询计划",
            }
            yield f"data: {json.dumps(plan_review_event, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
            session["state"] = accumulated_state
            session["waiting_for_plan_review"] = True
            yield "data: [DONE]\n\n"
            return

        if accumulated_state:
            session["state"] = accumulated_state
            final_state = accumulated_state
            need_clarification = final_state.get("ambiguity_detected", False)
            if update_waiting_for_clarification:
                session["waiting_for_clarification"] = need_clarification
                session["waiting_for_plan_review"] = False

            asyncio.create_task(
                record_log(
                    final_state,
                    workspace_id=final_state.get("workspace_id") or initial_state.get("workspace_id"),
                )
            )

            final_data = final_state.get("analysis_result")
            if not final_data:
                final_data = final_state.get("execution_result")

            # 获取数据元信息（用于前端分页展示）
            total_count = final_state.get("total_count", 0)
            is_truncated = final_state.get("is_truncated", False)

            result_data = {
                'type': 'result',
                'response': final_state.get("final_response", ""),
                'sql': final_state.get("generated_sql"),
                'python_code': final_state.get("analysis_code"),
                'need_clarification': need_clarification,
                'intent_type': _serialize_intent_type(final_state.get("intent_type")),
                'data': final_data,
                'total_count': total_count,
                'is_truncated': is_truncated,
                'suggested_questions': final_state.get("suggested_questions", [])
            }
            if include_sql_reflection:
                result_data['sql_reflection'] = final_state.get("sql_reflection")

            yield f"data: {json.dumps(result_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"

        yield "data: [DONE]\n\n"

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"流式执行错误: {error_detail}")
        message = str(e)
        if error_message_prefix:
            message = f"{error_message_prefix}{message}"
        error_data = {
            'type': 'error',
            'message': message
        }
        yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"


async def stream_graph_execution(graph, initial_state: AgentState, session: dict[str, Any]) -> AsyncGenerator[str, None]:
    """
    流式执行 LangGraph 并发送步骤更新

    Args:
        graph: LangGraph 实例
        initial_state: 初始状态
        session: 会话字典，用于保存状态

    Yields:
        SSE 格式的事件数据
    """
    async for chunk in _stream_graph_events(
        graph,
        initial_state,
        session,
        start_message="开始处理查询...",
        error_message_prefix="处理出错: ",
        include_sql_reflection=True,
        update_waiting_for_clarification=True,
    ):
        yield chunk


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """流式聊天接口。"""
    session = get_or_create_session(request.session_id, request.workspace_id)
    graph = session["graph"]

    if session["waiting_for_clarification"] and session["state"]:
        previous_state = session["state"]
        new_state: AgentState = {
            "user_query": previous_state.get("user_query", ""),
            "clarification_response": request.message,
            "messages": previous_state.get("messages", []) + [
                {"role": "assistant", "content": previous_state.get("clarification_question", "")},
                {"role": "user", "content": request.message},
            ],
            "clarification_count": previous_state.get("clarification_count", 0),
            "enable_suggestions": request.enable_suggestions,
            "workspace_id": previous_state.get("workspace_id") or request.workspace_id or DEFAULT_WORKSPACE_ID,
        }
        session["waiting_for_clarification"] = False

        async def clarification_stream() -> AsyncGenerator[str, None]:
            async for chunk in _stream_graph_events(
                graph,
                new_state,
                session,
                start_message="处理澄清回复...",
                use_accumulated_query_plan=True,
            ):
                yield chunk

        return StreamingResponse(clarification_stream(), media_type="text/event-stream")

    session["state"] = None
    session["waiting_for_clarification"] = False
    session["waiting_for_plan_review"] = False
    # 使用前端传来的 thread_id，保留对话历史；若无则生成新的
    session["current_thread_id"] = request.thread_id or f"query_{uuid.uuid4().hex[:8]}"

    initial_state: AgentState = {
        "user_query": request.message,
        "messages": [("user", request.message)],
        "clarification_count": 0,
        "enable_suggestions": request.enable_suggestions,
        "workspace_id": request.workspace_id or DEFAULT_WORKSPACE_ID,
    }

    return StreamingResponse(
        stream_graph_execution(graph, initial_state, session),
        media_type="text/event-stream",
    )


async def record_log(state: AgentState, workspace_id: Optional[str] = None):
    """异步记录轨迹日志，兼容 VALUE 与 METRIC 两条路径。"""
    try:
        from tools import log_trajectory, generate_trajectory_id
        effective_workspace_id = workspace_id or state.get("workspace_id")

        tid = state.get("trajectory_id")
        if not tid:
            tid = generate_trajectory_id()

        intent_type = state.get("intent_type", "")
        intent_type_str = str(intent_type)
        is_metric = any(keyword in intent_type_str for keyword in ["metric_query", "METRIC_QUERY", "Metric"])

        if is_metric:
            log_trajectory(
                trajectory_id=tid,
                user_query=state.get("user_query") or "",
                intent_type=str(intent_type),
                query_plan=state.get("query_plan"),
                analysis_code=None,
                analysis_result=None,
                analysis_error=None,
                verification_passed=None,
                verification_feedback=None,
                metric_plan_nodes=state.get("metric_plan_nodes"),
                execution_history=state.get("execution_history"),
                step_results=state.get("step_results"),
                loop_status=state.get("loop_status"),
                metric_final_result=state.get("execution_result"),
                final_response=state.get("final_response"),
                ground_truth=None,
                reward=None,
                workspace_id=effective_workspace_id,
            )
        else:
            log_trajectory(
                trajectory_id=tid,
                user_query=state.get("user_query") or "",
                intent_type=str(intent_type),
                query_plan=state.get("query_plan"),
                generated_sql=state.get("generated_sql"),
                execution_result=state.get("execution_result"),
                execution_error=state.get("execution_error"),
                sql_reflection=state.get("sql_reflection"),
                final_response=state.get("final_response"),
                ground_truth=None,
                reward=None,
                workspace_id=effective_workspace_id,
            )
    except Exception as e:
        print(f"日志记录失败: {e}")


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """非流式聊天接口（保留兼容性）"""
    try:
        session = get_or_create_session(request.session_id, request.workspace_id)
        graph = session["graph"]

        # 配置递归限制
        thread_id = session.get("current_thread_id") or session.get("session_key", "default_thread")
        config: dict[str, object] = {
            "recursion_limit": 50,
            "configurable": {"thread_id": thread_id}
        }

        if session["waiting_for_clarification"] and session["state"]:
            result = process_clarification(graph, session["state"], request.message, config=config)
        else:
            initial_state: AgentState = {
                "user_query": request.message,
                "messages": [],
                "clarification_count": 0,
                "workspace_id": request.workspace_id or DEFAULT_WORKSPACE_ID,
            }
            result = graph.invoke(initial_state, config=config)

        session["state"] = result

        # 记录日志
        await record_log(result, workspace_id=request.workspace_id or result.get("workspace_id"))

        need_clarification = result.get("ambiguity_detected", False)
        if need_clarification:
            session["waiting_for_clarification"] = True
            response_text = result.get("clarification_question", "")
        else:
            session["waiting_for_clarification"] = False
            response_text = result.get("final_response", "")

        return ChatResponse(
            response=response_text,
            need_clarification=need_clarification,
            sql=result.get("generated_sql"),
            intent_type=_serialize_intent_type(result.get("intent_type"))
        )

    except Exception as e:
        import traceback
        print(f"非流式执行错误: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))


class ChartRequest(BaseModel):
    session_id: str
    workspace_id: Optional[str] = None


class ReplayRequest(BaseModel):
    session_id: str
    workspace_id: Optional[str] = None
    sql: Optional[str] = None
    python_code: Optional[str] = None


def _to_jsonable(data: Any) -> Any:
    """将任意对象转换为可 JSON 序列化的数据"""
    return json.loads(json.dumps(data, ensure_ascii=False, cls=CustomJSONEncoder))


def _normalize_tabular_payload(
    *,
    payload: Any = None,
    state: Optional[dict[str, Any]] = None,
) -> Optional[list[dict[str, Any]]]:
    """优先使用规范化结果；无法规范化时由调用方决定回退策略"""
    try:
        session_state = state or {}
        return normalize_canonical_tabular_result(
            payload=payload,
            analysis_result=session_state.get("analysis_result"),
            execution_result=session_state.get("execution_result"),
        )
    except Exception:
        return None


@app.post("/api/replay-data")
async def replay_data(request: ReplayRequest):
    """根据 SQL 或 Python 代码回放数据"""
    try:
        session = get_or_create_session(request.session_id, request.workspace_id)

        # 优先回放 Python 代码
        if request.python_code and request.python_code.strip():
            from agents.python_executor import execute_python_code, clean_code

            code = clean_code(request.python_code)
            data, error = execute_python_code(code)
            if error:
                return {"data": None, "error": f"回放失败: {error}"}

            if session.get("state"):
                state = session["state"]
                state["analysis_result"] = data
                session["state"] = state

            normalized_data = _normalize_tabular_payload(payload=data, state=session.get("state"))
            replay_data_payload = normalized_data if normalized_data is not None else data
            return {"data": _to_jsonable(replay_data_payload), "source": "python"}

        # 其次回放 SQL
        if request.sql and request.sql.strip():
            from tools.db_client import load_data

            df = load_data(request.sql)
            data = df.to_dict(orient='records')

            if session.get("state"):
                state = session["state"]
                state["execution_result"] = data
                session["state"] = state

            normalized_data = _normalize_tabular_payload(payload=data, state=session.get("state"))
            replay_data_payload = normalized_data if normalized_data is not None else data
            return {"data": _to_jsonable(replay_data_payload), "source": "sql"}

        # 没提供回放源时，回退到当前会话数据
        state = session.get("state") or {}
        data = _normalize_tabular_payload(state=state)
        if data is None:
            data = state.get("analysis_result") or state.get("execution_result")
        return {"data": _to_jsonable(data), "source": "session"}

    except Exception as e:
        import traceback
        print(f"数据回放错误: {traceback.format_exc()}")
        return {"data": None, "error": f"回放失败: {str(e)}"}


@app.post("/api/chart")
async def generate_chart(request: ChartRequest):
    """生成图表接口。"""
    try:
        session = get_or_create_session(request.session_id, request.workspace_id)
        if not session.get("state"):
            return {"chart_spec": None, "reasoning": "没有可用的查询结果"}

        state = session["state"]
        chart_data = _normalize_tabular_payload(state=state)
        if not chart_data:
            return {"chart_spec": None, "reasoning": "查询未返回数据，无法生成图表"}

        max_chart_records = 50
        if isinstance(chart_data, list) and len(chart_data) > max_chart_records:
            chart_data = chart_data[:max_chart_records]

        chart_state = state.copy()
        chart_state["execution_result"] = chart_data

        from agents.chart_generator import create_chart_generator

        chart_gen = create_chart_generator()
        return chart_gen(chart_state)
    except Exception as e:
        import traceback

        print(f"图表生成错误: {traceback.format_exc()}")
        return {"chart_spec": None, "reasoning": f"生成失败: {str(e)}"}



@app.post("/api/chat/cancel-plan")
async def cancel_plan_review(session_id: str = "default", workspace_id: Optional[str] = None, thread_id: Optional[str] = None):
    """取消计划审核等待状态，恢复正常流程"""
    session_key = _build_session_key(session_id, workspace_id)
    if session_key in sessions:
        sessions[session_key]["waiting_for_plan_review"] = False
        if thread_id:
            sessions[session_key]["current_thread_id"] = thread_id
    return {"message": "计划审核已取消"}


class PlanResumeRequest(BaseModel):
    session_id: str = "default"
    workspace_id: Optional[str] = None
    thread_id: Optional[str] = None
    approved: bool
    adjustments: Optional[str] = None


@app.post("/api/chat/resume-plan")
async def resume_plan_review(request: PlanResumeRequest):
    """使用 LangGraph Command(resume) 恢复计划审核"""
    from langgraph.types import Command

    session = get_or_create_session(request.session_id, request.workspace_id)
    if not session["waiting_for_plan_review"]:
        raise HTTPException(status_code=400, detail="当前无待审核计划")

    graph = session["graph"]
    session["waiting_for_plan_review"] = False

    # 优先使用前端传来的 thread_id，否则用 session 中保存的
    if request.thread_id:
        session["current_thread_id"] = request.thread_id
    thread_id = session.get("current_thread_id") or session.get("session_key", "default_thread")
    config_dict = {
        "recursion_limit": 50,
        "configurable": {"thread_id": thread_id}
    }

    # 构造 resume decision
    if request.approved:
        decision = {"approved": True, "adjustments": ""}
    else:
        decision = {"approved": False, "adjustments": request.adjustments or ""}

    async def resume_stream() -> AsyncGenerator[str, None]:
        async for chunk in _stream_graph_events_with_resume(
            graph,
            decision,
            session,
            config_dict,
        ):
            yield chunk

    return StreamingResponse(resume_stream(), media_type="text/event-stream")


async def _stream_graph_events_with_resume(
    graph,
    decision: dict[str, Any],
    session: dict[str, Any],
    config_dict: dict[str, Any],
) -> AsyncGenerator[str, None]:
    """使用 Command(resume) 从中断恢复并流式输出事件"""
    from langgraph.types import Command

    try:
        yield f"data: {json.dumps({'type': 'start', 'message': '继续处理计划审核...'}, ensure_ascii=False)}\n\n"
        await asyncio.sleep(0.1)

        # 从 checkpoint 获取完整状态作为 accumulated_state 的基础
        accumulated_state: dict[str, Any] = {}
        try:
            snapshot = graph.get_state(config_dict)
            if snapshot and snapshot.values:
                accumulated_state = dict(snapshot.values)
        except Exception as e:
            print(f"DEBUG: resume get_state for init failed: {e}")

        current_step = 1

        for event in graph.stream(Command(resume=decision), config=config_dict):
            for node_name, node_output in event.items():
                if node_output and isinstance(node_output, dict):
                    accumulated_state.update(node_output)

                if node_name not in NODE_DISPLAY_NAMES:
                    continue

                step_data = _build_step_data(
                    node_name,
                    node_output or {},
                    current_step,
                    accumulated_state=accumulated_state,
                )

                yield f"data: {json.dumps(step_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
                current_step += 1
                await asyncio.sleep(0.1)

        # 检查是否又有 interrupt（用户调整后的二次审核）
        interrupt_data = None
        try:
            snapshot = graph.get_state(config_dict)
            if snapshot and snapshot.tasks:
                for task in snapshot.tasks:
                    if task.interrupts:
                        interrupt_value = task.interrupts[0].value
                        if isinstance(interrupt_value, dict) and interrupt_value.get("plan_nodes"):
                            interrupt_data = interrupt_value
                            break
        except Exception as e:
            print(f"DEBUG: resume get_state check failed: {e}")

        if interrupt_data:
            plan_review_event = {
                "type": "plan_review",
                "plan_nodes": interrupt_data.get("plan_nodes", []),
                "message": "请审核调整后的查询计划",
            }
            yield f"data: {json.dumps(plan_review_event, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
            session["state"] = accumulated_state
            session["waiting_for_plan_review"] = True
            yield "data: [DONE]\n\n"
            return

        if accumulated_state:
            session["state"] = accumulated_state
            final_state = accumulated_state

            asyncio.create_task(
                record_log(
                    final_state,
                    workspace_id=final_state.get("workspace_id"),
                )
            )

            final_data = final_state.get("analysis_result")
            if not final_data:
                final_data = final_state.get("execution_result")

            total_count = final_state.get("total_count", 0)
            is_truncated = final_state.get("is_truncated", False)

            result_data = {
                'type': 'result',
                'response': final_state.get("final_response", ""),
                'sql': final_state.get("generated_sql"),
                'python_code': final_state.get("analysis_code"),
                'need_clarification': final_state.get("ambiguity_detected", False),
                'intent_type': _serialize_intent_type(final_state.get("intent_type")),
                'data': final_data,
                'total_count': total_count,
                'is_truncated': is_truncated,
                'suggested_questions': final_state.get("suggested_questions", [])
            }

            yield f"data: {json.dumps(result_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"

        yield "data: [DONE]\n\n"

    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"resume 流式执行错误: {error_detail}")
        error_data = {'type': 'error', 'message': str(e)}
        yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"


@app.post("/api/reset")
async def reset_session(session_id: str = "default", workspace_id: Optional[str] = None):
    """重置旧版会话。"""
    session_key = _build_session_key(session_id, workspace_id)
    if session_key in sessions:
        session = sessions[session_key]
        _cleanup_session_resources(session)
        del sessions[session_key]
    return {"message": "会话已重置"}


def _cleanup_session_resources(session: dict[str, Any]) -> None:
    """
    清理会话持有的资源

    注意：MetricDBConnectionManager 被 metric_executor 节点闭包捕获，
    无法直接访问。但我们依赖 MySQL TEMPORARY TABLE 的特性：
    连接断开时自动清理。

    这里清理的是会话级别的显式资源。
    """
    try:
        # 如果会话有 state，检查是否有需要清理的临时文件
        state = session.get("state")
        if state:
            data_file_path = state.get("data_file_path")
            if data_file_path:
                import os
                try:
                    if os.path.exists(data_file_path):
                        os.remove(data_file_path)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"清理临时文件失败: {data_file_path}, {e}")
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"清理会话资源时出错: {e}")


@app.on_event("startup")
async def startup_history_services() -> None:
    """启动时初始化登录与历史记录服务。"""
    try:
        _ensure_history_services_ready()
    except Exception as exc:
        logging.getLogger(__name__).warning("历史记录服务初始化失败: %s", exc)


@app.post("/api/auth/login")
async def login(request: LoginRequest):
    """校验用户名密码并签发访问令牌。"""
    _ensure_history_services_ready()
    try:
        user = authenticate_user(request.username, request.password)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc

    token = create_access_token(
        {"uid": user.id, "username": user.username, "status": user.status},
        config.auth.jwt_secret,
        expires_in_minutes=config.auth.jwt_expire_minutes,
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "username": user.username, "status": user.status},
    }


@app.get("/api/conversations")
async def list_history_conversations(
    current_user: AuthenticatedUser = Depends(_get_current_user),
):
    """返回当前登录用户的历史会话列表。"""
    _ensure_history_services_ready()
    return {"items": list_conversations(user_id=current_user.id)}


@app.get("/api/conversations/{conversation_id}/messages")
async def get_history_messages(
    conversation_id: str,
    current_user: AuthenticatedUser = Depends(_get_current_user),
):
    """返回指定会话的历史消息。"""
    _ensure_history_services_ready()
    conversation = _get_owned_conversation_or_404(current_user.id, conversation_id)
    messages = get_conversation_messages(user_id=current_user.id, conversation_id=conversation_id)
    return {"conversation": conversation, "messages": messages}


@app.post("/api/conversations/{conversation_id}/messages")
async def save_history_messages(
    conversation_id: str,
    request: SaveConversationMessagesRequest,
    current_user: AuthenticatedUser = Depends(_get_current_user),
):
    """保存一批新增历史消息；会话不存在时自动创建。"""
    _ensure_history_services_ready()
    if not request.messages:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="消息列表不能为空")

    conversation = ensure_conversation(
        user_id=current_user.id,
        conversation_id=conversation_id,
        workspace_id=request.workspace_id or DEFAULT_WORKSPACE_ID,
        session_id=request.session_id,
        title=request.title or "",
        enable_suggestions=request.enable_suggestions,
    )

    last_message_preview = conversation.get("last_message_preview") or ""
    for item in request.messages:
        append_chat_message(
            user_id=current_user.id,
            conversation_id=conversation_id,
            role=item.role,
            content=item.content,
            steps=item.steps,
            generated_sql=item.sql,
            python_code=item.pythonCode,
            need_clarification=item.needClarification,
            clarification_sections=item.clarificationSections,
            reflection=item.reflection,
            reasoning=item.reasoning,
            chart_reasoning=item.chartReasoning,
            chart_spec=item.chartSpec,
            sql_result=item.sqlResult,
            total_count=item.totalCount,
            is_truncated=item.isTruncated,
        )
        if item.content:
            last_message_preview = make_message_preview(item.content)

    update_conversation_after_message(
        user_id=current_user.id,
        conversation_id=conversation_id,
        title=request.title if request.title is not None else conversation.get("title") or "",
        suggested_questions=request.suggested_questions,
        enable_suggestions=request.enable_suggestions,
        last_message_preview=last_message_preview,
    )

    saved_conversation = get_conversation(user_id=current_user.id, conversation_id=conversation_id)
    saved_messages = get_conversation_messages(user_id=current_user.id, conversation_id=conversation_id)
    return {"conversation": saved_conversation, "messages": saved_messages}


@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "ok"}


@app.get("/api/metrics")
async def get_metrics():
    """获取指标体系数据"""
    try:
        metrics_path = config.paths.metrics_path
        with open(metrics_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"加载指标体系失败: {str(e)}")


@app.get("/api/schema")
async def get_schema():
    """获取数据库 Schema"""
    try:
        schema_path = config.paths.schema_path
        with open(schema_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"加载 Schema 失败: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时清理所有会话资源"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"应用关闭，清理 {len(sessions)} 个会话...")

    for session_key, session in list(sessions.items()):
        try:
            _cleanup_session_resources(session)
        except Exception as e:
            logger.warning(f"清理会话 {session_key} 失败: {e}")

    sessions.clear()
    logger.info("所有会话资源已清理")


# UI 目录路径
ui_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")

# 挂载静态文件目录
if os.path.exists(ui_dir):
    from fastapi.staticfiles import StaticFiles
    app.mount("/ui", StaticFiles(directory=ui_dir), name="ui")


@app.get("/login")
async def login_page():
    """登录页，直接返回 HTML 内容。"""
    login_path = os.path.join(ui_dir, "login.html")
    if os.path.exists(login_path):
        with open(login_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    return HTMLResponse(content="<h1>Login Page Not Found</h1>")


@app.get("/")
async def index():
    """首页，直接返回 HTML 内容。"""
    index_path = os.path.join(ui_dir, "index.html")
    if os.path.exists(index_path):
        with open(index_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        # 修正相对路径注入
        html_content = re.sub(r'href="style\.css(\?[^"]*)?', r'href="/ui/style.css\1', html_content)
        html_content = re.sub(r'src="script\.js(\?[^"]*)?', r'src="/ui/script.js\1', html_content)
        return HTMLResponse(content=html_content)
    return {"message": "Text2SQL API 服务运行中", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    print(f"UI Directory: {ui_dir}")
    print(f"Starting server at http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
