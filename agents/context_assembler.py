"""
上下文组装节点 - 使用 Query Planner 输出和 PromptBuilder 组装 Prompt
"""
import json
import os
from typing import Dict, Any, List, Optional

from state import AgentState, IntentType
from config import config
from prompts.prompt_builder import PromptBuilder
from prompts.domain_config import EducationDomain, DomainConfig


def load_schema() -> Dict[str, Any]:
    """加载数据库 Schema"""
    schema_path = config.paths.schema_path
    if os.path.exists(schema_path):
        with open(schema_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def load_full_metrics() -> Dict[str, Any]:
    """加载完整指标体系"""
    metrics_path = config.paths.metrics_path
    if os.path.exists(metrics_path):
        with open(metrics_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def filter_metrics_by_selection(full_metrics: Dict, selected_metrics: List[str]) -> str:
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
            level1_data = full_metrics[level1_name]
            
            if level2_name:
                # 二级指标
                level2_dict = level1_data.get("二级指标", {})
                if level2_name in level2_dict:
                    filtered_parts.append(f"### {level1_name} > {level2_name}")
                    filtered_parts.append(f"定义: {level2_dict[level2_name].get('二级指标解释', '')}")
            else:
                # 一级指标
                filtered_parts.append(f"### {level1_name}")
                filtered_parts.append(f"定义: {level1_data.get('一级指标解释', '')}")
                # 列出其下的二级指标
                level2_dict = level1_data.get("二级指标", {})
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


def extract_user_instructions(state: AgentState) -> List[str]:
    """从状态中提取用户指令"""
    instructions = []
    
    clarification_response = state.get("clarification_response")
    if clarification_response:
        instructions.append(f"用户补充说明: {clarification_response}")
    
    refined_intent = state.get("refined_intent")
    user_query = state.get("user_query", "")
    if refined_intent and refined_intent != user_query:
        instructions.append(f"明确的查询意图: {refined_intent}")
    
    return instructions


def create_context_assembler(prompt_builder: PromptBuilder = None):
    """
    创建上下文组装节点
    
    Args:
        prompt_builder: 提示词构建器（可选，如果未提供则内部创建）
    """
    
    def context_assembler_node(state: AgentState) -> Dict[str, Any]:
        """
        上下文组装节点
        
        接收 Query Planner 的输出，构建精简的 Prompt
        """
        user_query = state.get("user_query", "")
        refined_intent = state.get("refined_intent") or user_query
        
        # 获取 Query Planner 的输出
        query_plan = state.get("query_plan", {})
        reasoning_plan = state.get("reasoning_plan", "")
        selected_metrics = state.get("selected_metrics", [])
        
        # 加载资源
        schema = load_schema()
        full_metrics = load_full_metrics()
        
        # 根据选择的指标进行上下文剪枝
        if selected_metrics:
            filtered_metrics_text = filter_metrics_by_selection(full_metrics, selected_metrics)
        else:
            # 没有 Query Plan（可能是简单查询），使用全量
            filtered_metrics_text = json.dumps(full_metrics, ensure_ascii=False, indent=2)
        
        # 获取域配置和 PromptBuilder
        domain = get_domain_config()
        builder = prompt_builder or PromptBuilder(domain=domain)
        
        # 提取用户指令
        instructions = extract_user_instructions(state)
        
        # 添加 Query Plan 中的信息作为额外指令
        if query_plan:
            calc_type = query_plan.get("calculation_type", "")
            filters = query_plan.get("filters", {})
            group_by = query_plan.get("group_by", [])
            
            if calc_type:
                instructions.append(f"计算类型: {calc_type}")
            if filters:
                filter_str = ", ".join([f"{k}={v}" for k, v in filters.items() if v])
                if filter_str:
                    instructions.append(f"筛选条件: {filter_str}")
            if group_by:
                instructions.append(f"分组字段: {', '.join(group_by)}")
        
        # 使用 PromptBuilder 构建 Prompt
        assembled_prompt = builder.build_sql_generation_prompt(
            query=refined_intent,
            schema=schema,
            matched_metrics=None,  # 不再使用旧的 matched_metrics
            sql_samples=domain.sql_samples,
            instructions=instructions if instructions else None,
            reasoning_plan=reasoning_plan,
            full_metrics_context=filtered_metrics_text  # 使用筛选后的指标
        )
        
        return {
            "assembled_prompt": assembled_prompt,
            "current_node": "context_assembler"
        }
    
    return context_assembler_node
