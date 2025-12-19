"""
Text2SQL 智能体状态定义
"""
from typing import TypedDict, List, Optional, Dict, Any, Literal
from dataclasses import dataclass
from enum import Enum


class IntentType(str, Enum):
    """意图类型枚举"""
    VALUE_QUERY = "value_query"             # 数值查询（查询具体问题的原始数值）
    METRIC_QUERY = "metric_query"           # 指标聚合查询（需要聚合/加权计算）
    METRIC_DEFINITION = "metric_definition" # 询问指标定义/说明
    CHITCHAT = "chitchat"                   # 闲聊/帮助/指南
    
    # 向后兼容别名
    SIMPLE_QUERY = "value_query"            # 别名，保持兼容


@dataclass
class MetricInfo:
    """指标信息"""
    level1_name: str                  # 一级指标名称
    level1_description: str           # 一级指标描述
    level2_name: Optional[str] = None # 二级指标名称
    level2_description: Optional[str] = None  # 二级指标描述
    similarity_score: float = 0.0     # 向量检索相似度分数
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "level1_name": self.level1_name,
            "level1_description": self.level1_description,
            "level2_name": self.level2_name,
            "level2_description": self.level2_description,
            "similarity_score": self.similarity_score
        }


class AgentState(TypedDict, total=False):
    """LangGraph Agent 状态定义"""
    
    # 对话历史
    messages: List[Dict[str, str]]
    
    # 原始用户查询
    user_query: str
    
    # 向量检索结果 - 匹配到的指标列表
    matched_metrics: List[MetricInfo]
    
    # 意图分类结果
    intent_type: IntentType
    
    # 意图分类的详细分析
    intent_analysis: str
    
    # 是否检测到歧义
    ambiguity_detected: bool
    
    # 歧义详情
    ambiguity_details: List[str]
    
    # 澄清问题
    clarification_question: str
    
    # 用户澄清回复
    clarification_response: str
    
    # 澄清后的精确意图描述
    refined_intent: str
    
    # 组装后的 Prompt (用于微调模型)
    assembled_prompt: str
    
    # 生成的 SQL
    generated_sql: str
    
    # SQL 执行结果
    execution_result: Any
    
    # SQL 执行错误信息
    execution_error: Optional[str]
    
    # 最终回复
    final_response: str
    
    # 当前节点名称 (用于调试)
    current_node: str
    
    # 澄清轮次计数
    clarification_count: int
    
    # SQL 纠错相关
    correction_attempted: bool        # 是否尝试过纠错
    correction_count: int             # 纠错次数
    max_correction_attempts: int      # 最大纠错次数（默认2次）
    
    # 查询规划相关 (Query Planner)
    query_plan: Dict[str, Any]        # 结构化查询计划 (JSON)
    reasoning_plan: str               # 推理步骤文本
    selected_metrics: List[str]       # 规划器筛选出的指标列表
    
    # ReAct / 反思相关
    sql_reflection: Optional[str]     # SQL 执行后的反思思考过程
    execution_observation: Optional[str] # 格式化后的执行观测结果

