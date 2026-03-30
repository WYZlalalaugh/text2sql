"""
Schema 和指标体系缓存模块

避免每次请求重复读取和解析 JSON 文件。
模块级缓存：进程生命周期内只读一次磁盘。
"""
import json
import os
import logging
from typing import Dict, Any

try:
    from config import config
except ImportError:
    from ..config import config

logger = logging.getLogger(__name__)

# ============ 模块级缓存 ============
_schema_cache: Dict[str, Any] | None = None
_metrics_cache: Dict[str, Any] | None = None


def get_schema() -> Dict[str, Any]:
    """获取数据库 Schema (dict 格式，带缓存)"""
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = _load_json(config.paths.schema_path, "Schema")
    return _schema_cache


def get_schema_text() -> str:
    """获取数据库 Schema (JSON 字符串格式，带缓存)"""
    return json.dumps(get_schema(), ensure_ascii=False, indent=2)


def get_metrics() -> Dict[str, Any]:
    """获取完整指标体系 (dict 格式，带缓存)"""
    global _metrics_cache
    if _metrics_cache is None:
        _metrics_cache = _load_json(config.paths.metrics_path, "指标体系")
    return _metrics_cache


def get_metrics_text() -> str:
    """获取完整指标体系 (JSON 字符串格式，带缓存)"""
    return json.dumps(get_metrics(), ensure_ascii=False, indent=2)


def get_metrics_summary() -> str:
    """
    获取精简的指标摘要 (仅一级指标名称+描述)
    
    用于 intent_classifier 等不需要完整 JSON 的场景，
    大幅减少 prompt token 数。
    """
    metrics = get_metrics()
    if not metrics:
        return "无指标信息"
    
    lines = []
    for level1_name, level1_data in metrics.items():
        desc = level1_data.get("一级指标解释", "")
        level2_dict = level1_data.get("二级指标", {})
        level2_names = list(level2_dict.keys())
        level2_str = "、".join(level2_names) if level2_names else "无"
        lines.append(f"- **{level1_name}**: {desc} (包含: {level2_str})")
    
    return "\n".join(lines)


def invalidate_cache():
    """清除缓存，强制下次调用重新读取文件（用于热更新场景）"""
    global _schema_cache, _metrics_cache
    _schema_cache = None
    _metrics_cache = None
    logger.info("Schema/指标缓存已清除")


def _load_json(path: str, label: str) -> Dict[str, Any]:
    """安全加载 JSON 文件"""
    if not os.path.exists(path):
        logger.warning(f"{label}文件不存在: {path}")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            logger.info(f"{label}已加载并缓存: {path}")
            return data
    except Exception as e:
        logger.error(f"{label}加载失败: {path}, 错误: {e}")
        return {}
