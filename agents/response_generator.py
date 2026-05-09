"""
响应生成节点 - 将查询结果转换为自然语言回复
"""
import json
from decimal import Decimal
from datetime import datetime, date
from typing import Dict, Any, List

from state import AgentState, IntentType
from tools.schema_cache import get_metrics_text
from prompts import (
    CHITCHAT_PROMPT,
    QUERY_RESULT_PROMPT,
    GREETING_RESPONSE,
    HELP_RESPONSE
)


class SQLResultEncoder(json.JSONEncoder):
    """自定义 JSON 编码器，处理 MySQL 返回的特殊类型"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        return super().default(obj)


def _compute_statistics_summary(data_list: list) -> str:
    """
    对 list[dict] 结果集预计算统计摘要。
    返回人类可读的统计文本，供 LLM 参考做分析。
    """
    if not data_list or not isinstance(data_list, list):
        return ""
    
    n = len(data_list)
    all_keys = list(data_list[0].keys()) if data_list else []
    
    numeric_stats = {}
    categorical_stats = {}
    
    for key in all_keys:
        values = []
        non_none_count = 0
        cat_values = []
        for row in data_list:
            v = row.get(key)
            if v is None:
                continue
            non_none_count += 1
            if isinstance(v, (int, float)):
                values.append(float(v))
            elif isinstance(v, Decimal):
                values.append(float(v))
            else:
                cat_values.append(str(v))
        
        # 如果超过一半的非空值能解析为数值，视为数值列
        if values and len(values) > non_none_count * 0.5:
            sorted_vals = sorted(values)
            total = sum(sorted_vals)
            mean_val = total / len(sorted_vals)
            numeric_stats[key] = {
                "count": len(sorted_vals),
                "min": sorted_vals[0],
                "max": sorted_vals[-1],
                "mean": round(mean_val, 2),
                "sum": round(total, 2)
            }
        elif non_none_count > 0:
            unique = set(cat_values)
            if len(unique) <= 20:
                categorical_stats[key] = {"unique_count": len(unique), "values": sorted(unique)}
            else:
                categorical_stats[key] = {"unique_count": len(unique), "sample": sorted(unique)[:10]}
    
    lines = [f"【数据统计摘要】共 {n} 条记录，{len(all_keys)} 个字段"]
    
    if numeric_stats:
        lines.append("")
        lines.append("数值字段统计：")
        for key, stats in numeric_stats.items():
            lines.append(f"  - {key}: 最小={stats['min']}, 最大={stats['max']}, 均值={stats['mean']}, 总和={stats['sum']}, 有效数={stats['count']}")
    
    if categorical_stats:
        lines.append("")
        lines.append("分类字段统计：")
        for key, stats in categorical_stats.items():
            if "values" in stats:
                lines.append(f"  - {key}: {stats['unique_count']}个唯一值 -> {', '.join(stats['values'])}")
            else:
                lines.append(f"  - {key}: {stats['unique_count']}个唯一值（前10: {', '.join(stats['sample'])}）")
    
    return "\n".join(lines)


def _build_smart_result_str(data, max_sample: int = 15):
    """
    智能构建结果字符串：统计摘要 + 采样数据。
    
    返回 (result_str, is_summarized, total_count)
    - result_str: 供 LLM 消费的结果文本
    - is_summarized: 是否做了摘要（数据量大于 max_sample*2）
    - total_count: 原始数据总条数
    """
    if not isinstance(data, list) or len(data) == 0:
        try:
            result_str = json.dumps(data, ensure_ascii=False, indent=2, cls=SQLResultEncoder)
        except Exception:
            result_str = str(data)
        count = len(data) if isinstance(data, list) else 1
        return result_str, False, count
    
    total = len(data)
    
    if total <= max_sample * 2:
        # 数据量不大，全量序列化
        try:
            result_str = json.dumps(data, ensure_ascii=False, indent=2, cls=SQLResultEncoder)
        except Exception:
            result_str = str(data)
        return result_str, False, total
    
    # 数据量大：统计摘要 + 头尾采样
    summary = _compute_statistics_summary(data)
    
    head_sample = data[:max_sample]
    tail_sample = data[-5:]
    
    try:
        head_str = json.dumps(head_sample, ensure_ascii=False, indent=2, cls=SQLResultEncoder)
        tail_str = json.dumps(tail_sample, ensure_ascii=False, indent=2, cls=SQLResultEncoder)
    except Exception:
        head_str = str(head_sample)
        tail_str = str(tail_sample)
    
    result_str = (
        f"{summary}\n\n"
        f"【前 {max_sample} 条数据样本】\n{head_str}\n\n"
        f"【末尾 5 条数据样本】\n{tail_str}\n\n"
        f"[共 {total} 条数据。以上为统计摘要和采样，请基于摘要中的统计数据进行全面分析，"
        f"并提醒用户可通过\"查看数据\"按钮查看完整数据。]"
    )
    
    return result_str, True, total


def create_response_generator(llm_client=None):
    """
    创建响应生成节点
    
    Args:
        llm_client: LLM 客户端，用于生成自然语言回复
    """
    
    def response_generator_node(state: AgentState) -> Dict[str, Any]:
        """响应生成节点"""
        intent_type = state.get("intent_type", IntentType.CHITCHAT)
        user_query = state.get("user_query", "")
        
    # 0. 处理查询规划失败
        planning_error = state.get("planning_error")
        if planning_error:
            reply = f"抱歉，我无法理解您的查询并生成执行计划。\n\n{planning_error}\n\n建议您尝试：\n- 更明确地描述您想查询的内容\n- 指定具体的指标名称、地区或年份"
            return {
                "final_response": reply,
                "messages": [("assistant", reply)],
                "current_node": "response_generator"
            }
        
        # 根据意图类型处理
        if intent_type == IntentType.CHITCHAT:
            return generate_chitchat_response(state, llm_client)
        
        elif intent_type == IntentType.METRIC_DEFINITION:
            return generate_definition_response(state, llm_client)
        
        else:  # SIMPLE_QUERY 或 METRIC_QUERY
            return generate_query_response(state, llm_client)
    
    return response_generator_node


def generate_chitchat_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成闲聊回复"""
    user_query = state.get("user_query", "")
    
    if llm_client is not None:
        prompt = CHITCHAT_PROMPT.format(user_query=user_query)
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
    else:
        # 使用预设回复
        greetings = ["你好", "您好", "hi", "hello", "嗨", "早上好", "下午好", "晚上好"]
        if any(g in user_query.lower() for g in greetings):
            reply = GREETING_RESPONSE
        elif "帮助" in user_query or "help" in user_query.lower() or "怎么用" in user_query:
            reply = HELP_RESPONSE
        else:
            reply = "您好！请问有什么可以帮您的？我可以帮您查询学校数据和教育指标信息。"
    
    return {
        "final_response": reply,
        "messages": [("assistant", reply)],
        "current_node": "response_generator"
    }


def generate_definition_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成指标定义回复 - 基于全量指标体系"""
    user_query = state.get("user_query", "")
    
    # 加载全量指标 (使用缓存)
    full_metrics_text = get_metrics_text()
    if llm_client and full_metrics_text:
        # 使用 LLM 生成定义解释
        prompt = f"""你是一个教育指标专家。请根据以下指标体系定义，回答用户关于指标含义的问题。

### 指标体系
```json
{full_metrics_text}
```

### 用户问题
{user_query}

请直接回答指标的定义和解释，如果用户问到了具体的计算方式，也请一并说明。
"""
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
    else:
        # Fallback 到简单的关键词匹配（如果没有 LLM 或加载失败）
        reply = "抱歉，暂时无法查询指标 definition 信息。"
    
    return {
        "final_response": reply,
        "messages": [("assistant", reply)],
        "current_node": "response_generator"
    }


def generate_query_response(state: AgentState, llm_client) -> Dict[str, Any]:
    """生成查询结果回复"""
    user_query = state.get("user_query", "")
    generated_sql = state.get("generated_sql", "")
    execution_result = state.get("execution_result")
    execution_error = state.get("execution_error")
    
    # 新增: 获取数据分析结果 (Code-Based 模式)
    analysis_result = state.get("analysis_result")
    analysis_error = state.get("analysis_error")
    
    # 1. 优先处理分析错误
    if analysis_error:
        reply = f"数据分析遇到问题\n\n{analysis_error}\n"
        return {
            "final_response": reply,
            "messages": [("assistant", reply)],
            "current_node": "response_generator"
        }

    # 2. 处理分析结果 (METRIC_QUERY) — 使用智能摘要
    if analysis_result:
        result_str, is_summarized, total_count = _build_smart_result_str(
            analysis_result, max_sample=15
        )
            
        if llm_client is not None:
            prompt = QUERY_RESULT_PROMPT.format(
                user_query=user_query,
                result_data=result_str
            )
            response = llm_client.invoke(prompt)
            reply = response.content if hasattr(response, 'content') else str(response)
            
            # 如果数据做了摘要，在回复末尾追加提示
            if is_summarized:
                reply += f'\n\n> 以上分析基于 {total_count} 条完整数据的统计摘要。请点击下方"查看数据"按钮查看全部数据。'
        else:
            reply = f"分析完成，结果如下：\n\n{result_str}"
            if is_summarized:
                reply += f'\n\n> 完整数据共 {total_count} 条，请点击"查看数据"按钮查看。'
            
        return {
            "final_response": reply,
            "messages": [("assistant", reply)],
            "analysis_result": analysis_result,  # 透传完整分析结果（用于前端显示）
            "analysis_code": state.get("analysis_code"),  # 透传分析代码
            "current_node": "response_generator"
        }

    # 3. 处理 SQL 执行错误 (VALUE_QUERY)
    if execution_error:
        reply = f"查询执行遇到问题\n\n{execution_error}\n\n"
        if generated_sql:
            reply += f"生成的 SQL：\n```sql\n{generated_sql}\n```"
        return {
            "final_response": reply,
            "messages": [("assistant", reply)],
            "current_node": "response_generator"
        }
    
    # 4. 处理 SQL 执行结果 (VALUE_QUERY) — 同样使用智能摘要
    if not execution_result:
        reply = "查询完成，但没有找到符合条件的数据。\n\n"
        reply += "建议您：\n"
        reply += "- 放宽查询条件重试\n"
        reply += "- 检查年份、地区等筛选条件\n"
        if generated_sql:
            reply += f"\n执行的 SQL：\n```sql\n{generated_sql}\n```"
        return {
            "final_response": reply,
            "messages": [("assistant", reply)],
            "current_node": "response_generator" 
        }
    
    # 有 SQL 结果 — 使用智能摘要
    if llm_client is not None:
        result_str, is_summarized, total_count = _build_smart_result_str(
            execution_result, max_sample=10
        )
        prompt = QUERY_RESULT_PROMPT.format(
            user_query=user_query,
            result_data=result_str
        )
        response = llm_client.invoke(prompt)
        reply = response.content if hasattr(response, 'content') else str(response)
        
        if is_summarized:
            reply += f'\n\n> 以上分析基于 {total_count} 条完整数据的统计摘要。'
    else:
        # 简单格式化
        result_count = len(execution_result)
        reply = f"查询完成，共找到 {result_count} 条结果\n\n"
        for i, row in enumerate(execution_result[:5]):
            reply += f"{i+1}. {json.dumps(row, ensure_ascii=False)}\n"
        if result_count > 5:
            reply += f"\n... 还有 {result_count - 5} 条结果"
    
    intent_type = state.get("intent_type", IntentType.CHITCHAT)
    has_metric_plan = bool(state.get("metric_plan_nodes"))

    # metric query 下不附加 SQL（由前端历史汇总面板展示）
    if generated_sql and not has_metric_plan:
        reply += f"\n\n---\n执行的 SQL：\n```sql\n{generated_sql}\n```"
    
    # 限制前端数据量，避免传输过大的数据
    MAX_FRONTEND_RECORDS = 100
    total_count = len(execution_result) if isinstance(execution_result, list) else 0
    if isinstance(execution_result, list) and len(execution_result) > MAX_FRONTEND_RECORDS:
        frontend_data = execution_result[:MAX_FRONTEND_RECORDS]
        is_truncated = True
    else:
        frontend_data = execution_result
        is_truncated = False
    
    return {
        "final_response": reply,
        "messages": [("assistant", reply)],
        "execution_result": frontend_data,  # 限制后的数据（最多100条）
        "total_count": total_count,  # 总条数
        "is_truncated": is_truncated,  # 是否被截断
        "current_node": "response_generator"
    }


# 默认节点
response_generator_node = None
