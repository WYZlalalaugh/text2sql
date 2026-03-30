"""
迭代式指标循环类型定义
Iterative Metric Loop Type Definitions
"""
from typing import TypedDict, Literal, NotRequired


class MetricPlanNode(TypedDict):
    """指标计划节点 - 规划器输出的单个步骤意图"""
    step_id: str
    intent_type: Literal["filter", "aggregate", "join", "window", "derive"]
    description: str
    required_tables: list[str]
    depends_on: list[str]
    # 过滤条件列表，供 SQL 生成器直接消费
    # 约定元素示例: {"field": "province", "operator": "like", "value": "江苏"}
    filters: NotRequired[list[dict[str, object]]]
    expected_outputs: list[str]
    status: Literal["pending", "running", "completed", "failed"]


class StepResult(TypedDict):
    """步骤执行结果"""
    step_id: str
    generated_sql: str
    output_table: str | None
    row_count: int
    execution_time_ms: int
    error: str | None
    sample_rows: list[dict[str, object]] | None


class QualityIssue(TypedDict):
    """数据质量问题"""
    severity: Literal["info", "warning", "blocking"]
    category: str
    description: str
    affected_column: str | None


class DataSummary(TypedDict):
    """数据执行摘要"""
    row_count: int
    schema_snapshot: dict[str, str]
    grain_compliance: bool
    grain_violations: int
    sample_rows: list[dict[str, object]]
    column_statistics: dict[str, object]


class Observation(TypedDict):
    """观察器记录 - 单个步骤的观察结果"""
    step_id: str
    observation_type: Literal["success", "warning", "failed"]
    sql_executed: str
    execution_duration_ms: int
    error_summary: str | None
    error_category: Literal["SYNTAX_ERROR", "SCHEMA_MISMATCH", "PERFORMANCE", "PERMISSION", "OTHER"] | None
    fix_suggestion: str | None
    data_summary: DataSummary | None
    quality_issues: list[QualityIssue]
    timestamp: str


ObservationRecord = Observation


class LoopDecision(TypedDict):
    """规划器循环决策"""
    decision: Literal["continue", "adjust", "complete", "fail"]
    reason: str
    next_step_id: str | None
    adjustments: list[dict[str, object]] | None


__all__ = [
    "MetricPlanNode",
    "StepResult",
    "QualityIssue",
    "DataSummary",
    "Observation",
    "ObservationRecord",
    "LoopDecision",
]
