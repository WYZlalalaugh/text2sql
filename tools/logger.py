"""
轨迹日志记录器 - 用于 RL 训练数据收集

职责:
1. 记录完整的推理轨迹 (Query -> Plan -> Code -> Result -> Feedback)
2. 以 JSONL 格式保存到 logs/ 目录
3. 支持训练/评估模式的奖励标记
"""
import json
import os
import uuid
from datetime import datetime
from typing import Any, Optional, Dict

# 日志目录路径
_log_dir: Optional[str] = None


def init_logger(log_dir: Optional[str] = None):
    """
    初始化日志记录器
    
    Args:
        log_dir: 日志目录路径，默认为 text2sql/logs
    """
    global _log_dir
    
    if log_dir is None:
        # 默认使用 text2sql/logs 目录
        _log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    else:
        _log_dir = log_dir
    
    # 确保目录存在
    os.makedirs(_log_dir, exist_ok=True)


def log_trajectory(
    trajectory_id: str,
    user_query: str,
    intent_type: Optional[str] = None,
    query_plan: Optional[Dict[str, Any]] = None,
    # VALUE 路径字段
    generated_sql: Optional[str] = None,
    execution_result: Optional[Any] = None,
    execution_error: Optional[str] = None,
    sql_reflection: Optional[str] = None,
    # METRIC 路径字段
    analysis_code: Optional[str] = None,
    analysis_result: Optional[Any] = None,
    analysis_error: Optional[str] = None,
    verification_passed: Optional[bool] = None,
    verification_feedback: Optional[str] = None,
    # 循环执行字段 (新的迭代式指标循环)
    metric_plan_nodes: Optional[list] = None,
    execution_history: Optional[list] = None,
    step_results: Optional[dict] = None,
    loop_status: Optional[str] = None,
    metric_final_result: Optional[Any] = None,
    # 通用字段
    final_response: Optional[str] = None,
    ground_truth: Optional[Any] = None,
    reward: Optional[float] = None,
    workspace_id: Optional[str] = None,
):
    """
    记录一条完整的轨迹数据
    
    支持 VALUE_QUERY 和 METRIC_QUERY 两种路径的字段记录
    """
    global _log_dir
    
    if _log_dir is None:
        init_logger()
    assert _log_dir is not None
    
    # 根据意图类型确定执行路径
    # 处理多种可能的格式: "metric_query", "METRIC_QUERY", "IntentType.METRIC_QUERY"
    intent_type_str = str(intent_type) if intent_type else ""
    is_metric = any(keyword in intent_type_str for keyword in 
                    ["metric_query", "METRIC_QUERY", "Metric"])
    
    # 构建轨迹记录 - 区分不同路径的字段
    record = {
        "trajectory_id": trajectory_id,
        "timestamp": datetime.now().isoformat(),
        "user_query": user_query,
        "intent_type": intent_type,
        "execution_path": "metric" if is_metric else "value",
        
        # 查询规划 (通用)
        "query_plan": query_plan or {},
        
        # VALUE 路径字段
        "value_path": {
            "generated_sql": generated_sql,
            "execution_result": _ensure_serializable(execution_result),
            "execution_error": execution_error,
            "sql_reflection": sql_reflection,
        } if not is_metric else None,
        
        # METRIC 路径字段 (包括新的循环执行)
        "metric_path": {
            # 旧字段（保持兼容性，但新的循环不使用）
            "analysis_code": analysis_code,
            "analysis_result": _ensure_serializable(analysis_result),
            "analysis_error": analysis_error,
            "verification_passed": verification_passed,
            "verification_feedback": verification_feedback,
            # 循环执行相关 - 新的迭代式循环核心字段
            "metric_plan_nodes": metric_plan_nodes,
            "execution_history": execution_history,  # 每步的执行历史（包含SQL）
            "step_results": _ensure_serializable(step_results),  # 每步的详细结果（包含SQL、行数等）
            "loop_status": loop_status,
            "final_result": _ensure_serializable(metric_final_result),
            # 提取每一步的SQL快照（方便查看）
            "sql_snapshots": _extract_sql_snapshots(step_results, execution_history),
        } if is_metric else None,
        
        # 通用字段
        "final_response": final_response,
        "ground_truth": _ensure_serializable(ground_truth),
        "reward": reward,
        "workspace_id": workspace_id,
    }
    
    # 写入 JSONL 文件 (按日期分文件)
    date_str = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(_log_dir, f"trajectory_{date_str}.jsonl")
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')


def generate_trajectory_id() -> str:
    """生成唯一的轨迹 ID"""
    return str(uuid.uuid4())[:8]


def _extract_sql_snapshots(step_results: Optional[dict], execution_history: Optional[list]) -> list[dict]:
    """
    从 step_results 和 execution_history 中提取每一步的 SQL 快照
    
    返回格式:
    [
        {
            "step_id": "s1",
            "sql": "SELECT ...",
            "status": "success",
            "row_count": 100,
            "execution_time_ms": 50
        },
        ...
    ]
    """
    snapshots = []
    
    if not step_results:
        return snapshots
    
    # 遍历每个步骤的结果
    for step_id, result in step_results.items():
        if isinstance(result, dict):
            snapshot = {
                "step_id": step_id,
                "sql": result.get("generated_sql", "")[:1000],  # 限制长度
                "status": result.get("status", "unknown"),
                "row_count": result.get("row_count", 0),
                "execution_time_ms": result.get("execution_time_ms", 0),
                "is_final_step": result.get("is_final_step", False),
            }
            snapshots.append(snapshot)
    
    # 按步骤ID排序（假设步骤ID格式为 s1, s2, s3...）
    snapshots.sort(key=lambda x: x["step_id"])
    
    return snapshots


def _ensure_serializable(obj: Any) -> Any:
    """确保对象可 JSON 序列化"""
    if obj is None:
        return None
    
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    if isinstance(obj, (list, tuple)):
        return [_ensure_serializable(item) for item in obj]
    
    if isinstance(obj, dict):
        return {str(k): _ensure_serializable(v) for k, v in obj.items()}
    
    # 尝试 pandas/numpy 转换
    try:
        import pandas as pd
        import numpy as np
        
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient='records')
        if isinstance(obj, pd.Series):
            return obj.to_list()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (np.integer, np.floating)):
            return float(obj)
    except ImportError:
        pass
    
    return str(obj)
