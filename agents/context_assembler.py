"""
上下文组装节点 - 使用 Query Planner 输出和 PromptBuilder 组装 Prompt
"""
# pyright: reportDeprecated=false, reportUnknownParameterType=false, reportMissingTypeArgument=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnnecessaryComparison=false
import json
from typing import cast, Optional

from state import AgentState, IntentType
from prompts.prompt_builder import PromptBuilder
from prompts.domain_config import EducationDomain, DomainConfig
from prompts.sql_samples import SQLSampleLibrary
from tools.schema_provider import get_schema_provider


def filter_metrics_by_selection(full_metrics: dict[str, object], selected_metrics: list[str]) -> str:
    """
    根据 Query Planner 选择的指标，从全量指标中提取相关部分
    
    Args:
        full_metrics: 完整指标体系 (dict)
        selected_metrics: Query Planner 筛选出的指标列表 (e.g., ["基础设施 > 网络"])
        
    Returns:
        筛选后的指标信息文本
    """
    if not selected_metrics:
        # 如果没有筛选结果，返回全部（退化为原逻辑）
        return json.dumps(full_metrics, ensure_ascii=False, indent=2)
    
    filtered_parts = []
    
    for metric_path in selected_metrics:
        # 解析 "一级 > 二级" 格式
        parts = [p.strip() for p in metric_path.split(">")]
        level1_name = parts[0] if len(parts) >= 1 else ""
        level2_name = parts[1] if len(parts) >= 2 else None
        
        # 在全量指标中查找
        if level1_name in full_metrics:
            level1_data = cast(dict[str, object], full_metrics[level1_name])
            
            if level2_name:
                # 二级指标
                level2_dict = cast(dict[str, dict[str, object]], level1_data.get("二级指标", {}))
                if level2_name in level2_dict:
                    filtered_parts.append(f"### {level1_name} > {level2_name}")
                    filtered_parts.append(f"定义: {level2_dict[level2_name].get('二级指标解释', '')}")
            else:
                # 一级指标
                filtered_parts.append(f"### {level1_name}")
                filtered_parts.append(f"定义: {level1_data.get('一级指标解释', '')}")
                # 列出其下的二级指标
                level2_dict = cast(dict[str, dict[str, object]], level1_data.get("二级指标", {}))
                if level2_dict:
                    filtered_parts.append("包含二级指标:")
                    for l2_name, l2_info in level2_dict.items():
                        filtered_parts.append(f"  - {l2_name}: {l2_info.get('二级指标解释', '')}")
            
            filtered_parts.append("")
    
    if filtered_parts:
        return "\n".join(filtered_parts)
    else:
        # 未能匹配，返回全部
        return json.dumps(full_metrics, ensure_ascii=False, indent=2)


def get_domain_config() -> DomainConfig:
    """获取域配置"""
    return EducationDomain()


def _parse_json_object(text: str) -> Optional[dict[str, object]]:
    """Best-effort parse for provider text that may still be JSON."""
    if not text:
        return None

    try:
        parsed = cast(object, json.loads(text))
    except json.JSONDecodeError:
        return None

    return parsed if isinstance(parsed, dict) else None


def extract_user_instructions(state: AgentState) -> list[str]:
    """从状态中提取用户指令"""
    instructions: list[str] = []
    
    clarification_response = state.get("clarification_response")
    if clarification_response:
        instructions.append(f"用户补充说明: {clarification_response}")
    
    refined_intent = state.get("refined_intent")
    user_query = state.get("user_query", "")
    if refined_intent and refined_intent != user_query:
        instructions.append(f"明确的查询意图: {refined_intent}")
    
    return instructions


def create_context_assembler(prompt_builder: Optional[PromptBuilder] = None):
    """
    创建上下文组装节点
    
    Args:
        prompt_builder: 提示词构建器（可选，如果未提供则内部创建）
    """
    
    def context_assembler_node(state: AgentState) -> dict[str, object]:
        """
        上下文组装节点
        
        接收 Query Planner 的输出，构建精简的 Prompt
        根据意图类型选择不同的 Prompt 构建策略:
        - METRIC_QUERY: 使用简单 SQL Prompt (只拉取数据, 不做复杂计算)
        - 其他: 使用原有的 SQL 生成 Prompt
        """
        user_query = state.get("user_query", "")
        refined_intent = state.get("refined_intent") or user_query
        intent_type = state.get("intent_type")
        
        # 获取 Query Planner 的输出
        query_plan = cast(dict[str, object], state.get("query_plan", {}))
        reasoning_plan_raw = state.get("reasoning_plan", "")
        reasoning_plan = str(reasoning_plan_raw) if reasoning_plan_raw else ""
        selected_metrics_raw = cast(list[object], state.get("selected_metrics", []))
        selected_metrics = [str(metric) for metric in selected_metrics_raw]

        # 加载资源
        schema_provider = get_schema_provider(state.get("workspace_id"))
        schema_text = schema_provider.get_schema_text()
        metrics_text_raw = schema_provider.get_metrics_text()  # pyright: ignore[reportAny]
        metrics_text = cast(str, metrics_text_raw)
        schema = _parse_json_object(schema_text) or {"schema_summary": schema_text}
        full_metrics = _parse_json_object(metrics_text)
        
        # 获取域配置和 PromptBuilder
        domain = get_domain_config()
        builder = prompt_builder or PromptBuilder(domain=domain)
        
        # 判断是否为指标查询
        is_metric_query = (intent_type == IntentType.METRIC_QUERY or 
                          intent_type == "metric_query")
        
        # 准备 schema_context (供 Data Analyzer 使用)
        schema_context = schema_text
        
        if is_metric_query:
            # 指标查询 (Code-Based 模式): 
            # 直接进入 data_analyzer，不需要生成 SQL prompt
            # data_analyzer 会使用 schema_context 和 query_plan 生成完整代码
            return {
                "assembled_prompt": "",  # Code-Based 模式不使用此字段
                "schema_context": schema_context,
                "current_node": "context_assembler"
            }
        else:
            # 非指标查询 (VALUE_QUERY): 使用原有的完整 SQL 生成 Prompt
            # 根据选择的指标进行上下文剪枝
            if selected_metrics and full_metrics:
                filtered_metrics_text = filter_metrics_by_selection(full_metrics, selected_metrics)
            elif metrics_text:
                filtered_metrics_text = metrics_text
            else:
                filtered_metrics_text = ""
            
            # 提取用户指令
            instructions = extract_user_instructions(state)
            sql_samples = domain.sql_samples or SQLSampleLibrary()
            
            # 添加 Query Plan 中的信息作为额外指令
            if query_plan:
                calc_type = str(query_plan.get("calculation_type", "") or "")
                filters_raw = query_plan.get("filters", {})
                filters = cast(dict[str, object], filters_raw) if isinstance(filters_raw, dict) else {}
                group_by_raw = query_plan.get("group_by", [])
                group_by = [str(field) for field in group_by_raw] if isinstance(group_by_raw, list) else []
                
                if calc_type:
                    instructions.append(f"计算类型: {calc_type}")
                if filters:
                    filter_str = ", ".join([f"{k}={v}" for k, v in filters.items() if v])
                    if filter_str:
                        instructions.append(f"筛选条件: {filter_str}")
                if group_by:
                    instructions.append(f"分组字段: {', '.join(group_by)}")
            
            assembled_prompt = builder.build_sql_generation_prompt(
                query=refined_intent,
                schema=schema,
                matched_metrics=[],
                sql_samples=sql_samples,
                instructions=instructions,
                reasoning_plan=reasoning_plan,
                full_metrics_context=filtered_metrics_text
            )
            
            return {
                "assembled_prompt": assembled_prompt,
                "schema_context": schema_context,
                "current_node": "context_assembler"
            }
    
    return context_assembler_node
