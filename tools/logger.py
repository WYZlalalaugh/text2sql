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
    query_plan: Optional[Dict[str, Any]] = None,
    analysis_code: Optional[str] = None,
    analysis_result: Optional[Any] = None,
    analysis_error: Optional[str] = None,
    verification_passed: Optional[bool] = None,
    verification_feedback: Optional[str] = None,
    ground_truth: Optional[Any] = None,
    reward: Optional[float] = None,
    workspace_id: Optional[str] = None,
):
    """
    记录一条轨迹数据
    
    Args:
        trajectory_id: 轨迹唯一标识
        user_query: 用户原始查询
        query_plan: 查询规划 (JSON)
        analysis_code: 生成的 Python 代码
        analysis_result: 代码执行结果
        analysis_error: 执行错误信息
        verification_passed: 验证是否通过
        verification_feedback: 验证反馈
        ground_truth: 标准答案 (可选)
        reward: 奖励值 (可选, 用于 RL)
    """
    global _log_dir
    
    if _log_dir is None:
        init_logger()
    assert _log_dir is not None
    
    # 构建轨迹记录
    record = {
        "trajectory_id": trajectory_id,
        "timestamp": datetime.now().isoformat(),
        "user_query": user_query,
        "query_plan": query_plan,
        "analysis_code": analysis_code,
        "analysis_result": _ensure_serializable(analysis_result),
        "analysis_error": analysis_error,
        "verification_passed": verification_passed,
        "verification_feedback": verification_feedback,
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
