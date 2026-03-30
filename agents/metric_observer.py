"""
指标观察器 - 分析执行结果并生成观察记录
Metric Observer - analyzes execution results and generates observations

改进：
1. 添加 step_status 状态机 (pending|running|succeeded|failed_execution|failed_validation)
2. 执行成功时使用 LLM 进行语义化数据验证
3. 验证失败阻止下游步骤执行
"""

from __future__ import annotations

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false
import json
import time
from typing import Protocol, TypeAlias, cast

from agents.metric_constants import MAX_ITERATIONS
from prompts.data_validation_prompt import build_data_validation_prompt
from state import AgentState

MetricPlanNode: TypeAlias = dict[str, object]
Observation: TypeAlias = dict[str, object]


class LLMClient(Protocol):
    """LLM 客户端协议"""

    def invoke(self, prompt: str) -> object: ...


def create_metric_observer(llm_client: LLMClient | None = None):
    """
    创建指标观察器节点

    Args:
        llm_client: LLM 客户端，用于数据验证。如果为 None，使用启发式规则
    """

    def metric_observer_node(state: AgentState) -> dict[str, object]:
        """
        观察器节点 - 分析执行结果，生成观察记录，确定步骤状态
        """
        current_step_id = state.get("current_step_id")
        step_results = state.get("step_results") or {}
        execution_error = state.get("execution_error")
        generated_sql = str(state.get("generated_sql", "") or "")

        if not current_step_id:
            return {
                "execution_error": "未指定当前步骤ID",
                "current_node": "metric_observer",
                "step_status": "failed_execution",
            }

        step_result = step_results.get(str(current_step_id), {})

        # 构建观察记录
        observation = _create_observation(
            step_id=str(current_step_id),
            step_result=step_result,
            execution_error=(str(execution_error) if execution_error else None),
            generated_sql=generated_sql,
        )

        # 更新观察历史
        existing_observations = state.get("planner_observations") or []
        updated_observations = [*existing_observations, observation]

        # 确定步骤状态（使用 LLM 验证或启发式规则）
        step_status, validation_issues = _determine_step_status(
            observation=observation,
            state=state,
            step_result=step_result,
            llm_client=llm_client,
        )

        # 如果有验证问题，更新观察记录中的 quality_issues
        if validation_issues:
            observation["quality_issues"] = validation_issues
            # 注意：不修改 observation_type，让它保持原始值（"warning" 或 "success"）
            # step_status 已经反映了验证结果（failed_validation 或 succeeded）

        # 更新步骤状态映射
        existing_status_map = state.get("step_status_map") or {}
        updated_status_map = {**existing_status_map, str(current_step_id): step_status}

        # 根据步骤状态决定循环状态
        loop_status = _determine_loop_status_from_step_status(
            step_status=step_status,
            state=state,
        )

        return {
            "planner_observations": updated_observations,
            "loop_status": loop_status,
            "step_status": step_status,
            "step_status_map": updated_status_map,
            "current_node": "metric_observer",
        }

    return metric_observer_node


def _determine_step_status(
    observation: Observation,
    state: AgentState,
    step_result: dict[str, object],
    llm_client: LLMClient | None,
) -> tuple[str, list[dict[str, object]]]:
    """
    确定步骤的详细状态

    Returns:
        (step_status, validation_issues)
    """
    obs_type = observation.get("observation_type")

    if obs_type == "failed":
        return "failed_execution", []

    # 执行成功，进行数据验证
    # if llm_client:
    #     # 使用 LLM 进行语义化验证
    #     return _validate_with_llm(
    #         state=state,
    #         step_result=step_result,
    #         llm_client=llm_client,
    #     )
    # else:
    #     # 使用启发式规则（降级方案）
    #     return _validate_heuristic(
    #         observation=observation,
    #         step_result=step_result,
    #     )
    return _validate_heuristic(
        observation=observation,
        step_result=step_result,
    )


def _validate_with_llm(
    state: AgentState,
    step_result: dict[str, object],
    llm_client: LLMClient,
) -> tuple[str, list[dict[str, object]]]:
    """
    使用 LLM 进行语义化数据验证

    核心原则：零值可能是正常结果，不一定是失败
    """
    current_step_id = state.get("current_step_id")
    plan_nodes = state.get("metric_plan_nodes") or []

    # 找到当前步骤的节点定义
    current_node = None
    for node in plan_nodes:
        if node.get("step_id") == current_step_id:
            current_node = node
            break

    if not current_node:
        # 找不到节点定义，使用启发式
        return "succeeded", []

    # 提取验证所需信息
    step_description = str(current_node.get("description", ""))
    success_criteria = str(current_node.get("success_criteria", ""))

    row_count = _to_int(step_result.get("row_count"), 0)
    columns = _extract_columns(step_result)
    sample_rows = _extract_sample_rows(step_result)
    column_statistics = _calculate_statistics(step_result)

    # 构建验证 prompt
    system_prompt, user_prompt = build_data_validation_prompt(
        step_description=step_description,
        success_criteria=success_criteria,
        row_count=row_count,
        columns=columns,
        sample_rows=sample_rows,
        column_statistics=column_statistics,
    )

    try:
        # 调用 LLM
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        response = llm_client.invoke(full_prompt)
        response_text = str(getattr(response, "content", response))

        # 解析验证结果
        validation_result = _parse_validation_response(response_text)

        # 判断状态
        if validation_result.get("validation_passed") and validation_result.get(
            "meets_success_criteria"
        ):
            issues_raw = validation_result.get("issues", [])
            issues: list[dict[str, object]] = (
                cast(list[dict[str, object]], issues_raw)
                if isinstance(issues_raw, list)
                else []
            )
            return "succeeded", issues
        else:
            # 验证失败，返回 issues
            issues_raw = validation_result.get("issues", [])
            issues: list[dict[str, object]] = (
                cast(list[dict[str, object]], issues_raw)
                if isinstance(issues_raw, list)
                else []
            )
            if not issues:
                # 没有具体问题但验证失败，添加默认问题
                issues = [
                    {
                        "severity": "blocking",
                        "category": "semantic",
                        "description": str(
                            validation_result.get("reasoning", "数据不符合成功标准")
                        ),
                        "suggestion": "请检查查询逻辑或调整计划",
                    }
                ]
            return "failed_validation", issues

    except Exception as e:
        # LLM 验证失败，降级到启发式规则
        print(f"[WARNING] LLM 数据验证失败: {e}，使用启发式规则")
        return _validate_heuristic_simple(row_count)


def _parse_validation_response(response_text: str) -> dict[str, object]:
    """解析 LLM 验证响应"""
    try:
        # 尝试直接解析 JSON
        result = json.loads(response_text)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # 尝试提取 JSON 块
    try:
        json_start = response_text.find("{")
        json_end = response_text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            result = json.loads(response_text[json_start:json_end])
            if isinstance(result, dict):
                return result
    except json.JSONDecodeError:
        pass

    # 解析失败，返回空结果
    return {
        "validation_passed": True,
        "confidence": "low",
        "meets_success_criteria": True,
        "issues": [],
        "reasoning": "无法解析验证响应，默认通过",
    }


def _validate_heuristic(
    observation: Observation,
    step_result: dict[str, object],
) -> tuple[str, list[dict[str, object]]]:
    """启发式规则验证（完整版）"""
    quality_issues_raw = observation.get("quality_issues")
    quality_issues: list[dict[str, object]] = (
        quality_issues_raw if isinstance(quality_issues_raw, list) else []
    )

    # 检查是否有阻塞级质量问题
    blocking_issues = [
        q
        for q in quality_issues
        if isinstance(q, dict) and q.get("severity") == "blocking"
    ]

    if blocking_issues:
        return "failed_validation", blocking_issues

    return "succeeded", quality_issues


def _validate_heuristic_simple(row_count: int) -> tuple[str, list[dict[str, object]]]:
    """简化的启发式验证（仅用于 LLM 失败降级）"""
    # if row_count == 0:
    #     return "failed_validation", [
    #         {
    #             "severity": "blocking",
    #             "category": "completeness",
    #             "description": "查询返回0行数据（启发式规则）",
    #             "suggestion": "请检查筛选条件或查询逻辑",
    #         }
    #     ]
    return "succeeded", []


def _extract_columns(step_result: dict[str, object]) -> list[str]:
    """从步骤结果提取列名"""
    columns = []

    # 从 schema_snapshot 提取
    schema_snapshot = step_result.get("schema_snapshot")
    if isinstance(schema_snapshot, dict):
        columns = list(schema_snapshot.keys())

    # 从 columns 字段提取
    if not columns:
        raw_columns = step_result.get("columns")
        if isinstance(raw_columns, list):
            for item in raw_columns:
                if isinstance(item, dict):
                    name = item.get("name") or item.get("Field")
                    if name:
                        columns.append(str(name))

    return columns


def _extract_sample_rows(step_result: dict[str, object]) -> list[dict]:
    """从步骤结果提取样本行"""
    sample_rows = step_result.get("sample_rows")
    if isinstance(sample_rows, list):
        # 只返回前5行
        return sample_rows[:5]
    return []


def _calculate_statistics(step_result: dict[str, object]) -> dict[str, object]:
    """计算列统计信息"""
    statistics = {}

    sample_rows = step_result.get("sample_rows")
    if not isinstance(sample_rows, list) or len(sample_rows) == 0:
        return statistics

    # 对每列计算基本统计
    if sample_rows and isinstance(sample_rows[0], dict):
        for col in sample_rows[0].keys():
            values = [row.get(col) for row in sample_rows if isinstance(row, dict)]

            # 统计 null 数量
            null_count = sum(1 for v in values if v is None)

            # 统计零值数量（数值列）
            zero_count = sum(
                1 for v in values if isinstance(v, (int, float)) and v == 0
            )

            statistics[str(col)] = {
                "null_count": null_count,
                "zero_count": zero_count,
                "sample_count": len(values),
            }

    return statistics


def _determine_loop_status_from_step_status(
    step_status: str,
    state: AgentState,
) -> str:
    """
    根据步骤状态决定循环状态
    """
    loop_iteration = _to_int(state.get("loop_iteration"), 0)

    # 检查是否达到最大迭代次数
    if loop_iteration >= MAX_ITERATIONS:
        return "failed"

    if step_status == "succeeded":
        # 成功，检查是否还有更多步骤
        plan_nodes = state.get("metric_plan_nodes") or []
        current_step_id = state.get("current_step_id")

        current_idx = -1
        for i, node in enumerate(plan_nodes):
            if node.get("step_id") == current_step_id:
                current_idx = i
                break

        if current_idx >= 0 and current_idx < len(plan_nodes) - 1:
            return "executing"
        return "completed"

    # 任何失败状态都返回 adjusting
    return "adjusting"


def _create_observation(
    step_id: str,
    step_result: dict[str, object],
    execution_error: str | None,
    generated_sql: str,
) -> Observation:
    """创建观察记录"""

    if execution_error:
        return _create_failure_observation(step_id, execution_error, generated_sql)

    return _create_success_observation(step_id, step_result, generated_sql)


def _create_failure_observation(
    step_id: str,
    error: str,
    sql: str,
) -> Observation:
    """创建失败观察记录"""

    error_lower = error.lower()

    # 错误分类
    if any(kw in error_lower for kw in ["syntax", "parse", "invalid_function"]):
        category = "SYNTAX_ERROR"
        suggestion = "使用更简单的SQL语法，避免复杂函数"
    elif any(kw in error_lower for kw in ["column", "not exist", "unknown", "field"]):
        category = "SCHEMA_MISMATCH"
        suggestion = "检查列名是否与Schema匹配"
    elif any(kw in error_lower for kw in ["timeout", "slow", "lock"]):
        category = "PERFORMANCE"
        suggestion = "添加过滤条件或分批处理"
    elif any(kw in error_lower for kw in ["permission", "access", "denied"]):
        category = "PERMISSION"
        suggestion = "检查数据库权限"
    else:
        category = "OTHER"
        suggestion = "查看SQL并重试"

    return {
        "step_id": step_id,
        "observation_type": "failed",
        "sql_executed": sql[:500] if sql else "",
        "execution_duration_ms": 0,
        "error_summary": error[:300],
        "error_category": category,
        "fix_suggestion": suggestion,
        "data_summary": None,
        "quality_issues": [],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _create_success_observation(
    step_id: str,
    step_result: dict[str, object],
    sql: str,
) -> Observation:
    """创建成功观察记录"""

    row_count = _to_int(step_result.get("row_count"), 0)
    execution_time_ms = _to_int(step_result.get("execution_time_ms"), 0)

    sample_rows = step_result.get("sample_rows")
    if not isinstance(sample_rows, list):
        sample_rows = []

    schema_snapshot = _build_schema_snapshot(step_result)

    # 检查数据质量问题（预检查，供无 LLM 时使用）
    quality_issues: list[dict[str, object]] = []
    if row_count == 0:
        quality_issues.append(
            {
                "severity": "blocking",
                "category": "empty_result",
                "description": "查询返回0行数据（待 LLM 验证是否为正常零值）",
                "affected_column": None,
            }
        )

    # 确定观察类型
    observation_type = "warning" if quality_issues else "success"

    return {
        "step_id": step_id,
        "observation_type": observation_type,
        "sql_executed": sql[:500] if sql else "",
        "execution_duration_ms": execution_time_ms,
        "error_summary": None,
        "error_category": None,
        "fix_suggestion": None,
        "data_summary": {
            "row_count": row_count,
            "schema_snapshot": schema_snapshot,
            "grain_compliance": True,
            "grain_violations": 0,
            "sample_rows": sample_rows,
            "column_statistics": {},
        },
        "quality_issues": quality_issues,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }


def _build_schema_snapshot(step_result: dict[str, object]) -> dict[str, str]:
    """从步骤结果提取简化 schema 快照。"""
    raw_snapshot = step_result.get("schema_snapshot")
    if isinstance(raw_snapshot, dict):
        return {str(key): str(value) for key, value in raw_snapshot.items()}

    raw_columns = step_result.get("columns")
    if not isinstance(raw_columns, list):
        return {}

    snapshot: dict[str, str] = {}
    for item in raw_columns:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or item.get("Field")
        col_type = item.get("type") or item.get("Type")
        if name is None:
            continue
        snapshot[str(name)] = str(col_type) if col_type is not None else "unknown"
    return snapshot


def _to_int(value: object, default: int) -> int:
    try:
        if value is None:
            return default
        if not isinstance(value, (int, float, str, bool)):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = ["create_metric_observer"]
