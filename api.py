"""
Text2SQL API 服务 - FastAPI 后端（支持流式响应）
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncGenerator, Any
import os
import re
import json
import asyncio
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
from graph import process_clarification
from runtime_bootstrap import create_runtime_graph  # pyright: ignore[reportMissingImports]
from tools.result_normalizer import normalize_canonical_tabular_result

app = FastAPI(title="Text2SQL 智能体", version="1.0.0")

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
    enable_suggestions: bool = False  # 新增推荐开关
    workspace_id: Optional[str] = None


class ChatResponse(BaseModel):
    response: str
    need_clarification: bool
    sql: Optional[str] = None
    intent_type: Optional[str] = None


# 会话存储
DEFAULT_WORKSPACE_ID = "default"
sessions = {}


def _normalize_workspace_id(workspace_id: Optional[str]) -> str:
    return workspace_id or DEFAULT_WORKSPACE_ID


def _build_session_key(session_id: str, workspace_id: Optional[str] = None) -> str:
    normalized_workspace_id = _normalize_workspace_id(workspace_id)
    return f"{normalized_workspace_id}:{session_id}"


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
            "waiting_for_clarification": False
        }

    return sessions[session_key]


# 节点名称映射（用于前端显示）
NODE_DISPLAY_NAMES = {
    "vector_search": "向量检索",
    "intent_classifier": "意图识别",
    "ambiguity_checker": "歧义检测",
    "query_planner": "查询规划",
    "context_assembler": "上下文组装",
    "sql_generator": "SQL生成",
    "sql_executor": "SQL执行",
    "sql_corrector": "SQL纠错",
    "data_analyzer": "代码生成",      # 新增
    "python_executor": "代码执行",    # 新增
    "verifier": "结果验证",           # 新增
    "response_generator": "响应生成"
}


def _build_step_data(
    node_name: str,
    node_output: dict[str, Any],
    current_step: int,
    *,
    accumulated_state: AgentState,
    use_accumulated_query_plan: bool = False,
) -> dict[str, Any]:
    """构建单个 step 事件数据，保持 SSE 载荷结构稳定"""
    step_data = {
        'type': 'step',
        'step': current_step,
        'node': node_name,
        'title': NODE_DISPLAY_NAMES[node_name],
        'status': 'complete',
        'message': f"{NODE_DISPLAY_NAMES[node_name]}完成"
    }

    if node_name == "intent_classifier" and "intent_type" in node_output:
        intent_val = node_output['intent_type']
        intent_str = intent_val.value if hasattr(intent_val, 'value') else str(intent_val)
        step_data['detail'] = f"识别意图: {intent_str}"
    elif node_name == "query_planner":
        plan = node_output.get('query_plan', {})
        reasoning = node_output.get('reasoning_plan', '')
        if use_accumulated_query_plan:
            plan = node_output.get('query_plan') or accumulated_state.get('query_plan', {})
            reasoning = node_output.get('reasoning_plan') or accumulated_state.get('reasoning_plan', '')

        selected = plan.get('selected_metrics', []) if isinstance(plan, dict) else []
        if not isinstance(selected, list):
            selected = []
        calc = plan.get('calculation_type', '') if isinstance(plan, dict) else ''

        if selected:
            step_data['detail'] = f"筛选指标: {', '.join(selected[:2])}{'...' if len(selected) > 2 else ''}"
        elif calc:
            step_data['detail'] = f"计算类型: {calc}"
        else:
            step_data['detail'] = "正在规划查询路径..."

        if reasoning:
            step_data['reasoning'] = reasoning
    elif node_name == "sql_generator" and "generated_sql" in node_output:
        step_data['detail'] = "SQL 语句已生成"
        step_data['sql'] = node_output.get('generated_sql', '')
    elif node_name == "sql_executor":
        if "execution_result" in node_output:
            results = node_output.get('execution_result', [])
            if results:
                step_data['detail'] = f"查询返回 {len(results)} 条结果"
            else:
                step_data['detail'] = "查询没返回结果"
        elif "execution_error" in node_output:
            step_data['detail'] = "执行出错，准备纠错"
    elif node_name == "sql_corrector":
        step_data['detail'] = "AI 正在对执行结果进行反思和修正..."
        if "sql_reflection" in node_output:
            step_data['reflection'] = node_output.get('sql_reflection', '')
        if "generated_sql" in node_output:
            step_data['sql'] = node_output.get('generated_sql', '')
    elif node_name == "data_analyzer":
        if "analysis_code" in node_output and node_output.get("analysis_code"):
            step_data['detail'] = f"已生成分析代码 ({len(node_output.get('analysis_code', ''))} 字符)"
            step_data['python_code'] = node_output.get("analysis_code", "")
        elif "analysis_error" in node_output:
            step_data['detail'] = "代码生成遇到问题"
        else:
            step_data['detail'] = "正在生成分析代码..."
    elif node_name == "python_executor":
        if "analysis_result" in node_output and node_output.get("analysis_result"):
            result = node_output.get("analysis_result")
            if isinstance(result, list):
                step_data['detail'] = f"代码执行成功，返回 {len(result)} 条结果"
            else:
                step_data['detail'] = "代码执行成功"
        elif "analysis_error" in node_output:
            step_data['detail'] = f"执行出错: {node_output.get('analysis_error', '')[:50]}..."
        else:
            step_data['detail'] = "正在执行分析代码..."
    elif node_name == "verifier":
        if node_output.get("verification_passed"):
            step_data['detail'] = "验证通过"
        else:
            feedback = node_output.get("verification_feedback", "")
            step_data['detail'] = f"验证中: {feedback[:50]}..." if feedback else "验证中..."

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
                if node_output:
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

        if accumulated_state:
            session["state"] = accumulated_state
            final_state = accumulated_state
            need_clarification = final_state.get("ambiguity_detected", False)
            if update_waiting_for_clarification:
                session["waiting_for_clarification"] = need_clarification

            asyncio.create_task(
                record_log(
                    final_state,
                    workspace_id=final_state.get("workspace_id") or initial_state.get("workspace_id"),
                )
            )

            final_data = final_state.get("analysis_result")
            if not final_data:
                final_data = final_state.get("execution_result")

            result_data = {
                'type': 'result',
                'response': final_state.get("final_response", ""),
                'sql': final_state.get("generated_sql"),
                'python_code': final_state.get("analysis_code"),
                'need_clarification': need_clarification,
                'intent_type': str(final_state.get("intent_type", "")),
                'data': final_data,
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
    """流式聊天接口"""
    session = get_or_create_session(request.session_id, request.workspace_id)
    graph = session["graph"]
    
    if session["waiting_for_clarification"] and session["state"]:
        # 处理澄清回复 - 使用流式输出
        previous_state = session["state"]
        
        # 构建包含澄清信息的新状态
        new_state: AgentState = {
            "user_query": previous_state.get("user_query", ""),  # 保留原始查询
            "clarification_response": request.message,
            "messages": previous_state.get("messages", []) + [
                {"role": "assistant", "content": previous_state.get("clarification_question", "")},
                {"role": "user", "content": request.message}
            ],
            "clarification_count": previous_state.get("clarification_count", 0),
            "enable_suggestions": request.enable_suggestions,  # 传入推荐开关
            "workspace_id": previous_state.get("workspace_id") or request.workspace_id or DEFAULT_WORKSPACE_ID,
            # "refined_intent": request.message, 
        }
        
        # 更新会话状态
        session["waiting_for_clarification"] = False
        
        async def clarification_stream():
            async for chunk in _stream_graph_events(
                graph,
                new_state,
                session,
                start_message="处理澄清回复...",
                use_accumulated_query_plan=True,
            ):
                yield chunk
        
        return StreamingResponse(clarification_stream(), media_type="text/event-stream")
    else:
        # 新查询 - 重置会话状态，确保不继承上一轮的意图
        session["state"] = None  # 清空旧状态
        session["waiting_for_clarification"] = False
        
        # 生成新的 thread_id，确保 LangGraph 不从 checkpointer 恢复旧状态
        import uuid
        new_thread_id = f"query_{uuid.uuid4().hex[:8]}"
        session["current_thread_id"] = new_thread_id
        
        initial_state: AgentState = {
            "user_query": request.message,
            "messages": [("user", request.message)],
            "clarification_count": 0,
            "enable_suggestions": request.enable_suggestions,
            "workspace_id": request.workspace_id or DEFAULT_WORKSPACE_ID,
        }
        
        return StreamingResponse(
            stream_graph_execution(graph, initial_state, session),
            media_type="text/event-stream"
        )


async def record_log(state: AgentState, workspace_id: Optional[str] = None):
    """异步记录轨迹日志"""
    try:
        from tools import log_trajectory, generate_trajectory_id
        effective_workspace_id = workspace_id or state.get("workspace_id")
        
        # 确保有 ID
        tid = state.get("trajectory_id")
        if not tid:
            tid = generate_trajectory_id()
            
        log_trajectory(
            trajectory_id=tid,
            user_query=state.get("user_query") or "",
            query_plan=state.get("query_plan"),
            analysis_code=state.get("analysis_code"),
            analysis_result=state.get("analysis_result"),
            analysis_error=state.get("analysis_error"),
            verification_passed=state.get("verification_passed"),
            verification_feedback=state.get("verification_feedback"),
            ground_truth=None,  # 生产环境通常没有 GT
            reward=None,
            workspace_id=effective_workspace_id,
        )
        # print(f"轨迹日志已记录: {tid}")
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
            intent_type=str(result.get("intent_type", ""))
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
    """生成图表接口"""
    try:
        session = get_or_create_session(request.session_id, request.workspace_id)
        
        # 检查是否有状态
        if not session.get("state"):
            return {"chart_spec": None, "reasoning": "没有可用的查询结果"}
            
        state = session["state"]

        chart_data = _normalize_tabular_payload(state=state)

        if not chart_data:
            return {"chart_spec": None, "reasoning": "查询未返回数据，无法生成图表"}
        
        # 限制传入图表生成器的数据量，避免超出上下文
        MAX_CHART_RECORDS = 50
        if isinstance(chart_data, list) and len(chart_data) > MAX_CHART_RECORDS:
            chart_data = chart_data[:MAX_CHART_RECORDS]
            
        # 创建一个临时状态，包含截断后的数据
        chart_state = state.copy()
        chart_state["execution_result"] = chart_data  # chart_generator 使用 execution_result
            
        # 动态导入防止循环依赖
        from agents.chart_generator import create_chart_generator
        chart_gen = create_chart_generator()
        
        # 调用生成器
        result = chart_gen(chart_state)
        
        return result
        
    except Exception as e:
        import traceback
        print(f"图表生成错误: {traceback.format_exc()}")
        return {"chart_spec": None, "reasoning": f"生成失败: {str(e)}"}



@app.post("/api/reset")
async def reset_session(session_id: str = "default", workspace_id: Optional[str] = None):
    """重置会话"""
    session_key = _build_session_key(session_id, workspace_id)
    if session_key in sessions:
        del sessions[session_key]
    return {"message": "会话已重置"}


@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "ok"}


# UI 目录路径
ui_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")

# 挂载静态文件目录
if os.path.exists(ui_dir):
    from fastapi.staticfiles import StaticFiles
    app.mount("/ui", StaticFiles(directory=ui_dir), name="ui")


@app.get("/")
async def index():
    """首页 - 直接返回 HTML 内容"""
    index_path = os.path.join(ui_dir, "index.html")
    if os.path.exists(index_path):
        with open(index_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        # 修改静态资源路径（支持带版本号）
        html_content = re.sub(r'href="style\.css(\?[^"]*)?', r'href="/ui/style.css\1', html_content)
        html_content = re.sub(r'src="script\.js(\?[^"]*)?', r'src="/ui/script.js\1', html_content)
        return HTMLResponse(content=html_content)
    return {"message": "Text2SQL API 服务运行中", "docs": "/docs"}


if __name__ == "__main__":
    import uvicorn
    print(f"UI Directory: {ui_dir}")
    print(f"Starting server at http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
