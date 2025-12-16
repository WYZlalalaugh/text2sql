"""
Text2SQL API 服务 - FastAPI 后端（支持流式响应）
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel
from typing import Optional, AsyncGenerator
import os
import re
import json
import asyncio

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


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = "default"


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
    "response_generator": "响应生成"
}


async def stream_graph_execution(graph, initial_state: AgentState, session: dict) -> AsyncGenerator[str, None]:
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
        final_state = None
        
        # 配置递归限制
        config = {"recursion_limit": 50}
        
        for event in graph.stream(initial_state, config=config):
            # event 是 {node_name: node_output} 的字典
            for node_name, node_output in event.items():
                # 保存最新状态
                final_state = node_output
                
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
                            step_data['detail'] = "生成查询计划"
                    elif node_name == "sql_generator" and "generated_sql" in node_output:
                        step_data['detail'] = "SQL 语句已生成"
                        step_data['sql'] = node_output.get('generated_sql', '')
                    elif node_name == "sql_executor":
                        if "query_results" in node_output:
                            results = node_output.get('query_results', [])
                            step_data['detail'] = f"查询返回 {len(results)} 条结果"
                        elif "execution_error" in node_output:
                            step_data['detail'] = "执行出错，准备纠错"
                    
                    yield f"data: {json.dumps(step_data, ensure_ascii=False)}\n\n"
                    current_step += 1
                    await asyncio.sleep(0.1)
        
        # 发送最终结果并更新会话状态
        if final_state:
            # 更新会话状态
            session["state"] = final_state
            need_clarification = final_state.get("ambiguity_detected", False)
            session["waiting_for_clarification"] = need_clarification
            
            result_data = {
                'type': 'result',
                'response': final_state.get("final_response", ""),
                'sql': final_state.get("generated_sql"),
                'need_clarification': need_clarification,
                'intent_type': str(final_state.get("intent_type", ""))
            }
            yield f"data: {json.dumps(result_data, ensure_ascii=False)}\n\n"
        
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
            "refined_intent": request.message,  # 用澄清回复作为精炼意图
        }
        
        # 更新会话状态
        session["waiting_for_clarification"] = False
        
        async def clarification_stream():
            try:
                yield f"data: {json.dumps({'type': 'start', 'message': '处理澄清回复...'}, ensure_ascii=False)}\n\n"
                
                # 重新运行 Graph
                current_step = 1
                final_state = None
                config_dict = {"recursion_limit": 50}
                
                for event in graph.stream(new_state, config=config_dict):
                    for node_name, node_output in event.items():
                        final_state = node_output
                        
                        if node_name in NODE_DISPLAY_NAMES:
                            step_data = {
                                'type': 'step',
                                'step': current_step,
                                'node': node_name,
                                'title': NODE_DISPLAY_NAMES[node_name],
                                'status': 'complete',
                                'message': f"{NODE_DISPLAY_NAMES[node_name]}完成"
                            }
                            
                            if node_name == "sql_generator" and "generated_sql" in node_output:
                                step_data['detail'] = "SQL 语句已生成"
                                step_data['sql'] = node_output.get('generated_sql', '')
                            
                            yield f"data: {json.dumps(step_data, ensure_ascii=False)}\n\n"
                            current_step += 1
                            await asyncio.sleep(0.1)
                
                # 更新会话状态
                if final_state:
                    session["state"] = final_state
                    
                    result_data = {
                        'type': 'result',
                        'response': final_state.get("final_response", ""),
                        'sql': final_state.get("generated_sql"),
                        'need_clarification': final_state.get("ambiguity_detected", False)
                    }
                    yield f"data: {json.dumps(result_data, ensure_ascii=False)}\n\n"
                
                yield "data: [DONE]\n\n"
                
            except Exception as e:
                import traceback
                print(f"澄清处理错误: {traceback.format_exc()}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)}, ensure_ascii=False)}\n\n"
        
        return StreamingResponse(clarification_stream(), media_type="text/event-stream")
    else:
        # 新查询
        initial_state: AgentState = {
            "user_query": request.message,
            "messages": [],
            "clarification_count": 0
        }
        
        return StreamingResponse(
            stream_graph_execution(graph, initial_state, session),
            media_type="text/event-stream"
        )


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
