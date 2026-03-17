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

from config import config
from state import AgentState
from graph import create_graph, process_clarification
from main import create_llm_client, create_embedding_client, OllamaEmbeddingClient

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


class ChatResponse(BaseModel):
    response: str
    need_clarification: bool
    sql: Optional[str] = None
    intent_type: Optional[str] = None


# 会话存储
sessions = {}


def get_or_create_session(session_id: str):
    """获取或创建会话"""
    if session_id not in sessions:
        llm_client = create_llm_client()
        embedding_client = OllamaEmbeddingClient()
        
        graph = create_graph(
            llm_client=llm_client,
            embedding_client=embedding_client
        )
        
        sessions[session_id] = {
            "session_id": session_id,
            "graph": graph,
            "state": None,
            "waiting_for_clarification": False
        }
    
    return sessions[session_id]


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
    try:
        # 发送开始事件
        yield f"data: {json.dumps({'type': 'start', 'message': '开始处理查询...'}, ensure_ascii=False)}\n\n"
        await asyncio.sleep(0.1)
        
        # 使用 stream 方法执行图
        current_step = 1
        # 初始化累积状态，确保不丢失初始信息
        accumulated_state = initial_state.copy()
        
        # 配置递归限制和会话 ID
        # 使用动态生成的 thread_id，避免 checkpointer 恢复旧状态
        thread_id = session.get("current_thread_id") or session.get("session_id", "default_thread")
        config = {
            "recursion_limit": 50,
            "configurable": {"thread_id": thread_id}
        }
        
        for event in graph.stream(initial_state, config=config):
            # event 是 {node_name: node_output} 的字典
            for node_name, node_output in event.items():
                # 合并最新状态
                if node_output:
                    accumulated_state.update(node_output)
                
                if node_name in NODE_DISPLAY_NAMES:
                    step_data = {
                        'type': 'step',
                        'step': current_step,
                        'node': node_name,
                        'title': NODE_DISPLAY_NAMES[node_name],
                        'status': 'complete',
                        'message': f"{NODE_DISPLAY_NAMES[node_name]}完成"
                    }
                    
                    # 添加特定节点的详细信息
                    if node_name == "intent_classifier" and "intent_type" in node_output:
                        intent_val = node_output['intent_type']
                        intent_str = intent_val.value if hasattr(intent_val, 'value') else str(intent_val)
                        step_data['detail'] = f"识别意图: {intent_str}"
                    elif node_name == "query_planner":
                        # 提取 Query Plan 详情
                        plan = node_output.get('query_plan', {})
                        selected = plan.get('selected_metrics', [])
                        calc = plan.get('calculation_type', '')
                        
                        if selected:
                            step_data['detail'] = f"筛选指标: {', '.join(selected[:2])}{'...' if len(selected) > 2 else ''}"
                        elif calc:
                            step_data['detail'] = f"计算类型: {calc}"
                        else:
                            step_data['detail'] = "正在规划查询路径..."
                        
                        # 重要：同时抓取推理计划用于流式展示
                        if "reasoning_plan" in node_output:
                            step_data['reasoning'] = node_output.get('reasoning_plan', '')

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
                    
                    # 新增: 处理 Code-Based 模式的节点
                    elif node_name == "data_analyzer":
                        if "analysis_code" in node_output and node_output.get("analysis_code"):
                            code_preview = node_output.get("analysis_code", "")[:100]
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

                    
                    yield f"data: {json.dumps(step_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
                    current_step += 1
                    await asyncio.sleep(0.1)
        
        # 发送最终结果并更新会话状态
        if accumulated_state:
            # 更新会话状态（保存完整的累积状态）
            session["state"] = accumulated_state
            final_state = accumulated_state # 为了兼容下面的代码
            need_clarification = final_state.get("ambiguity_detected", False)
            session["waiting_for_clarification"] = need_clarification
            
            # 记录轨迹日志
            asyncio.create_task(record_log(final_state))
            
            # 智能选择数据源: 优先使用 analysis_result (Code-Based), 其次 execution_result (SQL-Based)
            final_data = final_state.get("analysis_result")
            if not final_data:
                final_data = final_state.get("execution_result")
            
            result_data = {
                'type': 'result',
                'response': final_state.get("final_response", ""),
                'sql': final_state.get("generated_sql"),
                'python_code': final_state.get("analysis_code"), # 新增: 返回 Python 代码
                'need_clarification': need_clarification,
                'intent_type': str(final_state.get("intent_type", "")),
                'sql_reflection': final_state.get("sql_reflection"), 
                'data': final_data,  # 统一数据字段
                'suggested_questions': final_state.get("suggested_questions", [])
            }
            yield f"data: {json.dumps(result_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
        
        # 发送完成标记
        yield "data: [DONE]\n\n"
        
    except Exception as e:
        import traceback
        error_detail = traceback.format_exc()
        print(f"流式执行错误: {error_detail}")
        error_data = {
            'type': 'error',
            'message': f'处理出错: {str(e)}'
        }
        yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """流式聊天接口"""
    session = get_or_create_session(request.session_id)
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
            "enable_suggestions": request.enable_suggestions # 传入推荐开关
            # "refined_intent": request.message, 
        }
        
        # 更新会话状态
        session["waiting_for_clarification"] = False
        
        async def clarification_stream():
            try:
                yield f"data: {json.dumps({'type': 'start', 'message': '处理澄清回复...'}, ensure_ascii=False)}\n\n"
                
                # 重新运行 Graph
                current_step = 1
                # 初始化累积状态
                accumulated_state = new_state.copy()
                
                thread_id = session.get("session_id", "default_thread")
                config_dict = {
                    "recursion_limit": 50,
                    "configurable": {"thread_id": thread_id}
                }
                
                for event in graph.stream(new_state, config=config_dict):
                    for node_name, node_output in event.items():
                        # 合并最新状态
                        if node_output:
                            accumulated_state.update(node_output)
                        
                        if node_name in NODE_DISPLAY_NAMES:
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
                                plan = node_output.get('query_plan') or accumulated_state.get('query_plan', {})
                                selected = plan.get('selected_metrics', []) if isinstance(plan, dict) else []
                                calc = plan.get('calculation_type', '') if isinstance(plan, dict) else ''

                                if selected:
                                    step_data['detail'] = f"筛选指标: {', '.join(selected[:2])}{'...' if len(selected) > 2 else ''}"
                                elif calc:
                                    step_data['detail'] = f"计算类型: {calc}"
                                else:
                                    step_data['detail'] = "正在规划查询路径..."

                                reasoning = node_output.get('reasoning_plan') or accumulated_state.get('reasoning_plan', '')
                                if reasoning:
                                    step_data['reasoning'] = reasoning

                            elif node_name == "sql_generator" and "generated_sql" in node_output:
                                step_data['detail'] = "SQL 语句已生成"
                                step_data['sql'] = node_output.get('generated_sql', '')

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

                            yield f"data: {json.dumps(step_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
                            current_step += 1
                            await asyncio.sleep(0.1)
                
                # 更新会话状态
                if accumulated_state:
                    session["state"] = accumulated_state
                    final_state = accumulated_state # 兼容下方变量名
                    
                    # 记录轨迹日志
                    asyncio.create_task(record_log(final_state))
                    
                    # 智能选择数据源: 优先使用 analysis_result, 其次 execution_result
                    final_data = final_state.get("analysis_result")
                    if not final_data:
                        final_data = final_state.get("execution_result")
                    
                    result_data = {
                        'type': 'result',
                        'response': final_state.get("final_response", ""),
                        'sql': final_state.get("generated_sql"),
                        'python_code': final_state.get("analysis_code"),  # 新增
                        'need_clarification': final_state.get("ambiguity_detected", False),
                        'intent_type': str(final_state.get("intent_type", "")),
                        'data': final_data,  # 修正: 统一数据字段
                        'suggested_questions': final_state.get("suggested_questions", [])
                    }
                    yield f"data: {json.dumps(result_data, ensure_ascii=False, cls=CustomJSONEncoder)}\n\n"
                
                yield "data: [DONE]\n\n"
                
            except Exception as e:
                import traceback
                print(f"澄清处理错误: {traceback.format_exc()}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
        
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
            "enable_suggestions": request.enable_suggestions
        }
        
        return StreamingResponse(
            stream_graph_execution(graph, initial_state, session),
            media_type="text/event-stream"
        )


async def record_log(state: AgentState):
    """异步记录轨迹日志"""
    try:
        from tools import log_trajectory, generate_trajectory_id
        
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
            reward=None
        )
        # print(f"轨迹日志已记录: {tid}")
    except Exception as e:
        print(f"日志记录失败: {e}")


@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """非流式聊天接口（保留兼容性）"""
    try:
        session = get_or_create_session(request.session_id)
        graph = session["graph"]
        
        # 配置递归限制
        config = {"recursion_limit": 50}
        
        if session["waiting_for_clarification"] and session["state"]:
            result = process_clarification(graph, session["state"], request.message)
        else:
            initial_state: AgentState = {
                "user_query": request.message,
                "messages": [],
                "clarification_count": 0
            }
            result = graph.invoke(initial_state, config=config)
        
        session["state"] = result
        
        # 记录日志
        await record_log(result)
        
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


class ReplayRequest(BaseModel):
    session_id: str
    sql: Optional[str] = None
    python_code: Optional[str] = None


def _to_jsonable(data: Any) -> Any:
    """将任意对象转换为可 JSON 序列化的数据"""
    return json.loads(json.dumps(data, ensure_ascii=False, cls=CustomJSONEncoder))


@app.post("/api/replay-data")
async def replay_data(request: ReplayRequest):
    """根据 SQL 或 Python 代码回放数据"""
    try:
        session = get_or_create_session(request.session_id)

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

            return {"data": _to_jsonable(data), "source": "python"}

        # 其次回放 SQL
        if request.sql and request.sql.strip():
            from tools.db_client import load_data

            df = load_data(request.sql)
            data = df.to_dict(orient='records')

            if session.get("state"):
                state = session["state"]
                state["execution_result"] = data
                session["state"] = state

            return {"data": _to_jsonable(data), "source": "sql"}

        # 没提供回放源时，回退到当前会话数据
        state = session.get("state") or {}
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
        session = get_or_create_session(request.session_id)
        
        # 检查是否有状态
        if not session.get("state"):
            return {"chart_spec": None, "reasoning": "没有可用的查询结果"}
            
        state = session["state"]
        
        # 智能选择数据源: 优先 analysis_result, 其次 execution_result
        chart_data = state.get("analysis_result") or state.get("execution_result")
        
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
async def reset_session(session_id: str = "default"):
    """重置会话"""
    if session_id in sessions:
        del sessions[session_id]
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
