"""
验证智能体 - 检查数据分析结果的正确性

职责:
1. 检查代码执行错误
2. 验证结果是否合理 (非空、数值范围等)
3. 支持 ground_truth 对比 (训练模式)
4. 决定是否需要重试或纠正
"""
from typing import Dict, Any, Optional
import math

from state import AgentState, IntentType


def create_verifier(llm_client=None):
    """
    创建验证智能体
    
    Args:
        llm_client: 可选的 LLM 客户端 (暂未使用)
    """
    
    def verifier_node(state: AgentState) -> Dict[str, Any]:
        """
        验证节点
        
        检查执行过程中的错误并决定下一步:
        1. 代码执行错误 -> 需要重新生成分析代码
        2. 结果异常 -> 需要反馈并可能重试
        3. ground_truth 不匹配 -> Fail (训练模式)
        4. 一切正常 -> 通过验证
        """
        # 获取相关状态
        analysis_error = state.get("analysis_error")
        analysis_result = state.get("analysis_result")
        verification_count = state.get("verification_count", 0)
        max_attempts = state.get("max_verification_attempts", 2)
        ground_truth = state.get("ground_truth")  # 可选的标准答案
        
        # 更新验证次数
        new_verification_count = verification_count + 1
        
        # 检查是否超过最大尝试次数
        if verification_count >= max_attempts:
            return {
                "verification_passed": False,
                "verification_feedback": f"已达到最大验证次数 ({max_attempts})，停止重试。",
                "verification_count": new_verification_count,
                "current_node": "verifier"
            }
        
        # 1. 检查代码执行错误
        if analysis_error:
            return {
                "verification_passed": False,
                "verification_feedback": f"代码执行错误: {analysis_error}",
                "verification_count": new_verification_count,
                "current_node": "verifier"
            }
        
        # 2. 检查结果是否存在
        if analysis_result is None:
            return {
                "verification_passed": False,
                "verification_feedback": "代码未返回结果 (result 为 None)。请确保代码正确赋值给 result 变量。",
                "verification_count": new_verification_count,
                "current_node": "verifier"
            }
        
        # 3. 检查结果有效性 (常规验证)
        result_check = _check_result_validity(analysis_result)
        if not result_check["valid"]:
            return {
                "verification_passed": False,
                "verification_feedback": result_check["message"],
                "verification_count": new_verification_count,
                "current_node": "verifier"
            }
        
        # 4. 语义验证: 检查结果是否符合查询计划的预期
        query_plan = state.get("query_plan", {})
        if query_plan:
            semantic_check = _check_semantic_validity(analysis_result, query_plan)
            if not semantic_check["valid"]:
                return {
                    "verification_passed": False,
                    "verification_feedback": f"语义验证未通过: {semantic_check['message']}",
                    "verification_count": new_verification_count,
                    "current_node": "verifier"
                }

        # 5. 如果提供了 ground_truth，进行对比验证 (训练模式)
        if ground_truth is not None:
            match_result = _compare_with_ground_truth(analysis_result, ground_truth)
            if not match_result["match"]:
                return {
                    "verification_passed": False,
                    "verification_feedback": f"结果与标准答案不匹配: {match_result['message']}",
                    "verification_count": new_verification_count,
                    "current_node": "verifier"
                }
        # 5. 所有检查通过
        return {
            "verification_passed": True,
            "verification_feedback": "验证通过，结果有效。",
            "verification_count": new_verification_count,
            "current_node": "verifier"
        }
    
    return verifier_node


def _check_result_validity(result: Any) -> Dict[str, Any]:
    """
    检查结果的有效性 (常规验证)
    
    Returns:
        {"valid": bool, "message": str}
    """
    if result is None:
        return {"valid": False, "message": "结果为 None"}
    
    if isinstance(result, (list, tuple)):
        if len(result) == 0:
            return {"valid": False, "message": "结果为空列表。请检查数据筛选条件或计算逻辑。"}
    
    if isinstance(result, dict):
        if len(result) == 0:
            return {"valid": False, "message": "结果为空字典。请检查计算逻辑。"}
    
    if isinstance(result, (int, float)):
        # 检查 NaN 或 Inf
        if math.isnan(result) or math.isinf(result):
            return {"valid": False, "message": f"结果为异常数值: {result}。请检查计算逻辑（可能存在除零或空数据）。"}
    
    return {"valid": True, "message": "结果有效"}


def _check_semantic_validity(result: Any, query_plan: dict) -> Dict[str, Any]:
    """
    语义验证: 检查结果是否符合查询计划的预期
    
    Args:
        result: 分析结果
        query_plan: 查询计划 (containing calculation_type, target_fields, etc.)
    
    Returns:
        {"valid": bool, "message": str}
    """
    calc_type = query_plan.get("calculation_type", "").lower()
    
    # 检查: 排名/对比类查询应该返回多条记录
    if calc_type in ("ranking", "comparison", "排名", "对比", "区域对比", "区域排名"):
        if isinstance(result, (list, tuple)) and len(result) <= 1:
            return {
                "valid": False,
                "message": f"查询计划要求'{calc_type}'，但结果只有 {len(result)} 条记录。请检查筛选条件是否过严，或是否遗漏了多区域/多学校的数据。"
            }
    
    # 检查: 结果中的数值是否全部为 0 (可疑模式)
    if isinstance(result, list) and len(result) > 1:
        _check = _detect_suspicious_values(result)
        if not _check["valid"]:
            return _check
    
    return {"valid": True, "message": "语义验证通过"}


def _detect_suspicious_values(result_list: list) -> Dict[str, Any]:
    """
    检测结果中的可疑模式 (e.g., 全零、全同值)
    
    仅对 list[dict] 格式的结果进行检查。
    """
    if not result_list or not isinstance(result_list[0], dict):
        return {"valid": True, "message": ""}
    
    # 提取所有数值列
    numeric_cols = {}
    for row in result_list:
        for k, v in row.items():
            if isinstance(v, (int, float)) and not math.isnan(v):
                numeric_cols.setdefault(k, []).append(v)
    
    for col_name, values in numeric_cols.items():
        if len(values) < 2:
            continue
        # 检查全零
        if all(v == 0 for v in values):
            return {
                "valid": False,
                "message": f"列 '{col_name}' 的所有值均为 0，可能查询了错误的字段或筛选条件有误。请检查 SQL 查询中的字段名和 WHERE 条件。"
            }
        # 检查全同值 (非零)
        if len(set(values)) == 1 and values[0] != 0:
            return {
                "valid": False,
                "message": f"列 '{col_name}' 的所有值均相同 ({values[0]})，可能查询了错误的字段或缺少 GROUP BY。"
            }
    
    return {"valid": True, "message": ""}

def _compare_with_ground_truth(result: Any, ground_truth: Any) -> Dict[str, Any]:
    """
    对比结果与标准答案 (训练模式)
    
    支持多种类型的智能对比:
    - 数值: 允许小误差 (1e-6)
    - 字符串: 精确匹配
    - 列表/字典: 递归比较
    
    Returns:
        {"match": bool, "message": str}
    """
    # 数值比较 (允许小误差)
    if isinstance(result, (int, float)) and isinstance(ground_truth, (int, float)):
        if abs(result - ground_truth) < 1e-6:
            return {"match": True, "message": "数值匹配"}
        else:
            return {"match": False, "message": f"期望 {ground_truth}, 实际 {result}"}
    
    # 字符串比较
    if isinstance(result, str) and isinstance(ground_truth, str):
        if result.strip() == ground_truth.strip():
            return {"match": True, "message": "字符串匹配"}
        else:
            return {"match": False, "message": f"字符串不匹配: 期望 '{ground_truth[:50]}...', 实际 '{result[:50]}...'"}
    
    # 列表比较 (简化版: 只比较长度和元素)
    if isinstance(result, list) and isinstance(ground_truth, list):
        if len(result) != len(ground_truth):
            return {"match": False, "message": f"列表长度不匹配: 期望 {len(ground_truth)}, 实际 {len(result)}"}
        # 简单比较 (不递归)
        if result == ground_truth:
            return {"match": True, "message": "列表匹配"}
        else:
            return {"match": False, "message": "列表内容不匹配"}
    
    # 字典比较
    if isinstance(result, dict) and isinstance(ground_truth, dict):
        if result == ground_truth:
            return {"match": True, "message": "字典匹配"}
        else:
            return {"match": False, "message": "字典内容不匹配"}
    
    # 类型不匹配
    if type(result) != type(ground_truth):
        return {"match": False, "message": f"类型不匹配: 期望 {type(ground_truth).__name__}, 实际 {type(result).__name__}"}
    
    # 默认精确比较
    if result == ground_truth:
        return {"match": True, "message": "匹配"}
    else:
        return {"match": False, "message": "不匹配"}


# 默认节点
verifier_node = None
