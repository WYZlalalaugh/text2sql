"""
指标循环规划器 - 迭代式执行循环的规划核心
Metric Loop Planner - generates initial plan and adjusts dynamically
"""
from __future__ import annotations

import json
import logging
import re
from typing import Protocol, TypeAlias, cast

from state import AgentState
from agents.metric_constants import MAX_ITERATIONS, MAX_RETRIES

logger = logging.getLogger(__name__)

LoopDecision: TypeAlias = dict[str, object]
PlanNode: TypeAlias = dict[str, object]
Observation: TypeAlias = dict[str, object]
StepResult: TypeAlias = dict[str, object]


class SupportsInvoke(Protocol):
    def invoke(self, prompt: str) -> object:
        ...


def create_metric_loop_planner(llm_client: SupportsInvoke | None = None):
    """
    创建指标循环规划器节点

    Args:
        llm_client: LLM客户端，用于生成和调整计划
    """

    def metric_loop_planner_node(state: AgentState) -> dict[str, object]:
        """
        循环规划节点 - 生成初始计划或根据观察结果调整计划

        职责:
        1. 首次进入：调用LLM生成初始计划（plan_nodes），等待用户审核
        2. 用户调整后：调用LLM重新生成计划，再次等待审核
        3. 执行成功：推进到下一步
        4. 执行失败：调用LLM调整计划或重试
        5. 循环结束：返回 complete 或 fail
        """
        loop_status = state.get("loop_status", "planning")
        plan_nodes = _coerce_plan_nodes(state.get("metric_plan_nodes"))
        observations = _coerce_observations(state.get("planner_observations"))
        retry_counters = _coerce_retry_counters(state.get("retry_counters"))
        loop_iteration = _coerce_int(state.get("loop_iteration"), 0)

        # ===== 用户调整计划后重新生成 =====
        plan_review_decision = state.get("plan_review_decision")
        if plan_review_decision and isinstance(plan_review_decision, dict):
            approved = plan_review_decision.get("approved")
            if approved is False or approved == "false":
                adjustments = _coerce_str(plan_review_decision.get("adjustments"), "")
                return _regenerate_plan_from_review(state, llm_client, adjustments, loop_iteration)

        # 检查最大迭代次数
        if loop_iteration >= MAX_ITERATIONS:
            return {
                "loop_decision": {"decision": "fail", "reason": "达到最大迭代次数"},
                "loop_status": "failed",
                "current_node": "metric_loop_planner",
            }

        # ===== 阶段1: 初始计划生成（仅当没有计划时） =====
        if not plan_nodes:
            return _generate_initial_plan(state, llm_client, loop_iteration)

        # ===== 阶段2: 根据观察结果决策 =====
        latest_observation = observations[-1] if observations else None

        if not latest_observation:
            # 没有观察记录，开始执行第一个步骤
            if plan_nodes:
                return {
                    "current_step_id": _coerce_str(plan_nodes[0].get("step_id"), ""),
                    "loop_decision": {
                        "decision": "continue",
                        "reason": "开始执行",
                        "next_step_id": _coerce_str(plan_nodes[0].get("step_id"), ""),
                    },
                    "loop_status": "executing",
                    "loop_iteration": loop_iteration + 1,
                    "current_node": "metric_loop_planner",
                }
            return {
                "loop_decision": {"decision": "fail", "reason": "没有计划节点"},
                "loop_status": "failed",
                "current_node": "metric_loop_planner",
            }

        obs_type = latest_observation.get("observation_type")
        obs_step_id = str(latest_observation.get("step_id") or "")

        if not obs_step_id:
            return {
                "loop_decision": {"decision": "fail", "reason": "观察记录缺少step_id"},
                "loop_status": "failed",
                "current_node": "metric_loop_planner",
            }

        # 获取步骤状态（优先使用 step_status，而不是 observation_type）
        step_status = state.get("step_status")
        step_status_map = state.get("step_status_map") or {}
        # 如果 step_status_map 中有当前步骤的状态，使用它
        if obs_step_id in step_status_map:
            step_status = step_status_map[obs_step_id]

        # ===== 阶段3: 处理失败 - 调用LLM调整计划 =====
        # 关键修复：基于 step_status 判断失败，而不仅仅是 observation_type
        if obs_type == "failed" or step_status in ("failed_execution", "failed_validation"):
            current_retries = retry_counters.get(obs_step_id, 0)

            if current_retries >= MAX_RETRIES:
                return {
                    "loop_decision": {"decision": "fail", "reason": f"步骤{obs_step_id}达到最大重试次数"},
                    "loop_status": "failed",
                    "current_node": "metric_loop_planner",
                }

            # 调用LLM分析错误并调整计划
            if llm_client:
                return _adjust_plan_with_llm(state, llm_client, obs_step_id, latest_observation, retry_counters, loop_iteration)

            # 没有LLM，简单重试
            retry_counters[obs_step_id] = current_retries + 1
            return {
                "current_step_id": obs_step_id,
                "retry_counters": retry_counters,
                "loop_decision": {"decision": "adjust", "reason": f"重试步骤{obs_step_id}", "next_step_id": obs_step_id},
                "loop_status": "adjusting",
                "loop_iteration": loop_iteration + 1,
                "current_node": "metric_loop_planner",
            }

        # ===== 阶段4: 成功 - 继续下一步 =====
        next_step = _select_next_runnable_step(plan_nodes, state, obs_step_id)
        if next_step:
            return {
                "current_step_id": next_step,
                "loop_decision": {"decision": "continue", "reason": "继续下一步", "next_step_id": next_step},
                "loop_status": "executing",
                "loop_iteration": loop_iteration + 1,
                "current_node": "metric_loop_planner",
            }

        # 所有步骤完成
        return {
            "loop_decision": {"decision": "complete", "reason": "所有步骤执行成功"},
            "loop_status": "completed",
            "current_node": "metric_loop_planner",
        }

    return metric_loop_planner_node


def _regenerate_plan_from_review(
    state: AgentState,
    llm_client: SupportsInvoke | None,
    adjustments: str,
    loop_iteration: int,
) -> dict[str, object]:
    """基于用户调整描述重新生成计划，保持原有格式，直接返回给用户再次审核"""
    from prompts.query_planner_prompt import build_plan_review_adjustment_prompt

    if not llm_client:
        return {
            "loop_decision": {"decision": "fail", "reason": "没有LLM客户端，无法重新生成计划"},
            "loop_status": "failed",
            "plan_review_decision": None,
            "plan_review_pending": None,
            "current_node": "metric_loop_planner",
        }

    original_plan = _coerce_plan_nodes(state.get("metric_plan_nodes"))

    try:
        schema_text = _resolve_schema_context(state)
        original_plan_json = json.dumps(original_plan, ensure_ascii=False)

        query = state.get("user_query", "")
        metrics_context = state.get("metrics_context")
        if metrics_context and isinstance(metrics_context, list) and len(metrics_context) > 0:
            metrics_str = json.dumps({"selected_indicators": metrics_context}, ensure_ascii=False)
        else:
            metrics_str = "{}"

        prompt = build_plan_review_adjustment_prompt(
            original_plan=original_plan_json,
            adjustments=adjustments,
            metrics=metrics_str,
            schema=schema_text,
            query=query,
        )

        response = llm_client.invoke(prompt)
        content = _response_to_text(response)
        plan_data = _extract_json(content)

        if not plan_data or "plan_nodes" not in plan_data:
            # LLM未能返回有效计划，保留原计划让用户重新审核
            logger.warning(f"计划调整失败: LLM返回格式无效，保留原计划")
            return {
                "metric_plan_nodes": original_plan,
                "loop_decision": {"decision": "awaiting_review", "reason": "计划调整失败，保留原计划"},
                "loop_status": "awaiting_review",
                "loop_iteration": loop_iteration + 1,
                "plan_review_decision": None,
                "plan_review_pending": None,
                "current_node": "metric_loop_planner",
            }

        new_plan_nodes = _coerce_plan_nodes(plan_data.get("plan_nodes"))

        if not new_plan_nodes:
            logger.warning(f"计划调整返回空计划，保留原计划")
            return {
                "metric_plan_nodes": original_plan,
                "loop_decision": {"decision": "awaiting_review", "reason": "调整计划为空，保留原计划"},
                "loop_status": "awaiting_review",
                "loop_iteration": loop_iteration + 1,
                "plan_review_decision": None,
                "plan_review_pending": None,
                "current_node": "metric_loop_planner",
            }

        logger.info(f"计划已根据用户调整重新生成，包含 {len(new_plan_nodes)} 个步骤")
        return {
            "metric_plan_nodes": new_plan_nodes,
            "loop_decision": {"decision": "awaiting_review", "reason": "计划已根据用户调整重新生成，等待审核"},
            "loop_status": "awaiting_review",
            "loop_iteration": loop_iteration + 1,
            "plan_review_decision": None,
            "plan_review_pending": None,
            "current_node": "metric_loop_planner",
        }

    except Exception as e:
        logger.error(f"重新生成计划失败: {e}")
        # 保留原计划让用户重新审核
        return {
            "metric_plan_nodes": original_plan,
            "loop_decision": {"decision": "awaiting_review", "reason": f"重新生成失败: {str(e)}，保留原计划"},
            "loop_status": "awaiting_review",
            "loop_iteration": loop_iteration + 1,
            "plan_review_decision": None,
            "plan_review_pending": None,
            "current_node": "metric_loop_planner",
        }


def _generate_initial_plan(
    state: AgentState,
    llm_client: SupportsInvoke | None,
    loop_iteration: int,
) -> dict[str, object]:
    """调用LLM生成初始计划，如果没有LLM则失败"""
    from prompts.query_planner_prompt import build_iterative_metric_planner_prompt

    schema_text = _resolve_schema_context(state)

    # 检查是否已有计划（从上游节点传入）
    existing_plan = state.get("metric_plan_nodes")
    if existing_plan:
        plan_nodes = _coerce_plan_nodes(existing_plan)
        if plan_nodes:
            return {
                "metric_plan_nodes": plan_nodes,
                "schema_context": schema_text,
                "current_step_id": _coerce_str(plan_nodes[0].get("step_id"), ""),
                "loop_decision": {
                    "decision": "awaiting_review",
                    "reason": "使用上游传入的计划，等待用户审核",
                    "next_step_id": _coerce_str(plan_nodes[0].get("step_id"), ""),
                },
                "loop_status": "awaiting_review",
                "loop_iteration": loop_iteration + 1,
                "current_node": "metric_loop_planner",
            }

    if not llm_client:
        return {
            "loop_decision": {"decision": "fail", "reason": "没有LLM客户端，无法生成计划"},
            "loop_status": "failed",
            "current_node": "metric_loop_planner",
        }

    try:
        # 构建提示词
        metrics_context = state.get("metrics_context")
        if metrics_context and isinstance(metrics_context, list) and len(metrics_context) > 0:
            # 将 ["一级=二级", ...] 格式转换为 JSON 字符串
            metrics = json.dumps({"selected_indicators": metrics_context}, ensure_ascii=False)
        else:
            metrics = "{}"
        query = state.get("user_query", "")

        prompt = build_iterative_metric_planner_prompt(
            metrics=str(metrics),
            schema=schema_text,
            query=query,
            execution_history="",
            observations="",
        )

        # 调用LLM
        response = llm_client.invoke(prompt)
        content = _response_to_text(response)

        # 解析JSON
        plan_data = _extract_json(content)

        if not plan_data or "plan_nodes" not in plan_data:
            return {
                "loop_decision": {"decision": "fail", "reason": "LLM返回的计划格式无效"},
                "loop_status": "failed",
                "current_node": "metric_loop_planner",
            }

        plan_nodes = _coerce_plan_nodes(plan_data.get("plan_nodes"))

        if not plan_nodes:
            return {
                "loop_decision": {"decision": "fail", "reason": "LLM返回的计划为空"},
                "loop_status": "failed",
                "current_node": "metric_loop_planner",
            }

        # 返回初始计划 - 等待用户审核
        return {
            "metric_plan_nodes": plan_nodes,
            "schema_context": schema_text,
            "current_step_id": plan_nodes[0].get("step_id"),
            "loop_decision": {
                "decision": "awaiting_review",
                "reason": "初始计划生成成功，等待用户审核",
                "next_step_id": plan_nodes[0].get("step_id"),
            },
            "loop_status": "awaiting_review",
            "loop_iteration": loop_iteration + 1,
            "current_node": "metric_loop_planner",
        }

    except Exception as e:
        logger.error(f"生成初始计划失败: {e}")
        return {
            "loop_decision": {"decision": "fail", "reason": f"生成计划失败: {str(e)}"},
            "loop_status": "failed",
            "current_node": "metric_loop_planner",
        }


def _adjust_plan_with_llm(
    state: AgentState,
    llm_client: SupportsInvoke,
    failed_step_id: str,
    observation: Observation,
    retry_counters: dict[str, int],
    loop_iteration: int,
) -> dict[str, object]:
    """调用LLM分析错误并调整计划"""
    from prompts.query_planner_prompt import build_iterative_metric_planner_prompt

    try:
        schema_text = _resolve_schema_context(state)

        # 构建执行历史
        execution_history = _build_execution_history(state, failed_step_id, observation)

        # 构建观察反馈 — 直接从 observation 读取原始错误，不做加工
        raw_error = _coerce_str(observation.get('raw_error', ''), '')
        sql_executed = _coerce_str(observation.get('sql_executed', ''), '')
        
        observations = (
            f"步骤 {failed_step_id} 执行失败:\n"
            f"原始错误:\n{raw_error}\n\n"
            f"执行SQL:\n{sql_executed}"
        )

        # 构建提示词
        metrics_context = state.get("metrics_context")
        if metrics_context and isinstance(metrics_context, list) and len(metrics_context) > 0:
            metrics_str = json.dumps({"selected_indicators": metrics_context}, ensure_ascii=False)
        else:
            metrics_str = "{}"
        
        prompt = build_iterative_metric_planner_prompt(
            metrics=metrics_str,
            schema=schema_text,
            query=state.get("user_query", ""),
            execution_history=execution_history,
            observations=observations,
        )

        # 调用LLM
        response = llm_client.invoke(prompt)
        content = _response_to_text(response)

        # 解析JSON
        plan_data = _extract_json(content)

        if not plan_data or "plan_nodes" not in plan_data:
            # LLM未能返回有效计划，简单重试
            retry_counters[failed_step_id] = retry_counters.get(failed_step_id, 0) + 1
            return {
                "current_step_id": failed_step_id,
                "retry_counters": retry_counters,
                "loop_decision": {"decision": "adjust", "reason": "LLM调整失败，重试", "next_step_id": failed_step_id},
                "loop_status": "adjusting",
                "loop_iteration": loop_iteration + 1,
                "current_node": "metric_loop_planner",
            }

        # 更新计划
        old_plan_nodes = _coerce_plan_nodes(state.get("metric_plan_nodes"))
        new_plan_nodes = _coerce_plan_nodes(plan_data.get("plan_nodes"))
        new_plan_nodes = _stabilize_failed_step_identity(new_plan_nodes, old_plan_nodes, failed_step_id)

        if not _plan_contains_step(new_plan_nodes, failed_step_id):
            logger.warning(
                "LLM调整后的计划缺少失败步骤ID=%s，回退到旧计划并重试",
                failed_step_id,
            )
            retry_counters[failed_step_id] = retry_counters.get(failed_step_id, 0) + 1
            return {
                "metric_plan_nodes": old_plan_nodes,
                "schema_context": schema_text,
                "current_step_id": failed_step_id,
                "retry_counters": dict(retry_counters),
                "loop_decision": {"decision": "adjust", "reason": "调整计划缺少失败步骤，回退后重试", "next_step_id": failed_step_id},
                "loop_status": "adjusting",
                "loop_iteration": loop_iteration + 1,
                "current_node": "metric_loop_planner",
            }

        # 增加重试计数
        retry_counters[failed_step_id] = retry_counters.get(failed_step_id, 0) + 1

        logger.info(f"计划已调整，新计划包含 {len(new_plan_nodes)} 个步骤")

        return {
            "metric_plan_nodes": new_plan_nodes,
            "schema_context": schema_text,
            "current_step_id": failed_step_id,  # 从失败步骤重新开始
            "retry_counters": dict(retry_counters),
            "loop_decision": {"decision": "continue", "reason": "计划已调整", "next_step_id": failed_step_id},
            "loop_status": "executing",
            "loop_iteration": loop_iteration + 1,
            "current_node": "metric_loop_planner",
        }

    except Exception as e:
        logger.error(f"调整计划失败: {e}")
        retry_counters[failed_step_id] = retry_counters.get(failed_step_id, 0) + 1
        return {
            "current_step_id": failed_step_id,
            "retry_counters": retry_counters,
            "loop_decision": {"decision": "adjust", "reason": f"调整失败: {str(e)}，重试", "next_step_id": failed_step_id},
            "loop_status": "adjusting",
            "loop_iteration": loop_iteration + 1,
            "current_node": "metric_loop_planner",
        }


def _build_execution_history(state: AgentState, failed_step_id: str, observation: Observation) -> str:
    """构建执行历史摘要"""
    plan_nodes = _coerce_plan_nodes(state.get("metric_plan_nodes"))
    step_results = _coerce_step_results(state.get("step_results"))
    execution_history = _coerce_execution_history(state.get("execution_history"))

    history_lines: list[str] = []

    attempts_by_step: dict[str, list[dict[str, object]]] = {}
    for record in execution_history:
        step_id = _coerce_str(record.get("step_id"), "")
        if step_id:
            attempts_by_step.setdefault(step_id, []).append(record)

    for node in plan_nodes:
        step_id = str(node.get("step_id", "unknown"))
        intent = node.get("intent_type", "unknown")
        desc = node.get("description", "")

        attempts = attempts_by_step.get(step_id, [])
        if attempts:
            for index, attempt in enumerate(attempts, start=1):
                status = _coerce_str(attempt.get("status"), "unknown")
                if status == "success":
                    result = step_results.get(step_id, {})
                    rows = result.get("row_count", 0)
                    history_lines.append(f"✓ {step_id}#{index} ({intent}): {desc} - 成功, {rows}行")
                else:
                    error = _coerce_str(attempt.get("error"), "未知错误")
                    history_lines.append(f"✗ {step_id}#{index} ({intent}): {desc} - 失败: {error}")
        elif step_id in step_results:
            result = step_results[step_id]
            status = _coerce_str(result.get("status"), "unknown")
            rows = result.get("row_count", 0)
            if status == "success":
                history_lines.append(f"✓ {step_id} ({intent}): {desc} - 成功, {rows}行")
            else:
                history_lines.append(f"✗ {step_id} ({intent}): {desc} - {status}")
        elif step_id == failed_step_id:
            obs_error = _coerce_str(observation.get("raw_error"), "未知错误")
            history_lines.append(f"✗ {step_id} ({intent}): {desc} - 失败: {obs_error}")
        else:
            history_lines.append(f"○ {step_id} ({intent}): {desc} - 未执行")

    return "\n".join(history_lines)


def _select_next_runnable_step(
    plan_nodes: list[PlanNode],
    state: AgentState,
    completed_step_id: str = "",
) -> str:
    """Pick the next unsatisfied step whose dependencies have all succeeded."""
    step_status_map_raw = state.get("step_status_map") or {}
    step_status_map = (
        step_status_map_raw if isinstance(step_status_map_raw, dict) else {}
    )
    step_results = _coerce_step_results(state.get("step_results"))
    observations = _coerce_observations(state.get("planner_observations"))

    succeeded_steps = {
        str(step_id)
        for step_id, status in step_status_map.items()
        if status == "succeeded"
    }
    succeeded_steps.update(
        step_id
        for step_id, result in step_results.items()
        if _coerce_str(result.get("status"), "") == "success"
    )
    succeeded_steps.update(
        _coerce_str(observation.get("step_id"), "")
        for observation in observations
        if _coerce_str(observation.get("observation_type"), "") in {"success", "warning"}
        and _coerce_str(observation.get("step_id"), "")
    )
    if completed_step_id:
        succeeded_steps.add(completed_step_id)
        completed_index = next(
            (
                index
                for index, node in enumerate(plan_nodes)
                if _coerce_str(node.get("step_id"), "") == completed_step_id
            ),
            -1,
        )
        if completed_index > 0:
            succeeded_steps.update(
                _coerce_str(node.get("step_id"), "")
                for node in plan_nodes[:completed_index]
                if _coerce_str(node.get("step_id"), "")
            )

    for node in plan_nodes:
        step_id = _coerce_str(node.get("step_id"), "")
        if not step_id or step_id in succeeded_steps:
            continue

        depends_on_raw = node.get("depends_on")
        depends_on = []
        if isinstance(depends_on_raw, list):
            depends_on = [
                _coerce_str(dep, "")
                for dep in cast(list[object], depends_on_raw)
                if _coerce_str(dep, "")
            ]
        if all(dep in succeeded_steps for dep in depends_on):
            return step_id

    return ""


def _stabilize_failed_step_identity(
    new_plan_nodes: list[PlanNode],
    old_plan_nodes: list[PlanNode],
    failed_step_id: str,
) -> list[PlanNode]:
    """稳定重试步骤ID：失败步骤在重规划后必须保持原ID。"""
    if _plan_contains_step(new_plan_nodes, failed_step_id):
        return new_plan_nodes

    remap_candidates: list[str] = []
    retry_prefix = f"{failed_step_id}_"

    for node in new_plan_nodes:
        node_step_id = _coerce_str(node.get("step_id"), "")
        if not node_step_id:
            continue
        if node_step_id.startswith(retry_prefix) or node_step_id.startswith(f"{failed_step_id}-"):
            remap_candidates.append(node_step_id)

    if len(remap_candidates) == 1:
        old_id = remap_candidates[0]
        remapped_nodes: list[PlanNode] = []
        for node in new_plan_nodes:
            copied_node = dict(node)
            if _coerce_str(copied_node.get("step_id"), "") == old_id:
                copied_node["step_id"] = failed_step_id

            depends_on_raw = copied_node.get("depends_on")
            if isinstance(depends_on_raw, list):
                updated_depends_on: list[str] = []
                depends_on_items = cast(list[object], depends_on_raw)
                for dep in depends_on_items:
                    dep_id = _coerce_str(dep, "")
                    updated_depends_on.append(failed_step_id if dep_id == old_id else dep_id)
                copied_node["depends_on"] = updated_depends_on

            remapped_nodes.append(copied_node)

        logger.info("重规划步骤ID已稳定化: %s -> %s", old_id, failed_step_id)
        return remapped_nodes

    if old_plan_nodes:
        logger.warning(
            "无法在新计划中定位失败步骤ID=%s的唯一重命名候选，保持新计划原样",
            failed_step_id,
        )
    return new_plan_nodes


def _plan_contains_step(plan_nodes: list[PlanNode], step_id: str) -> bool:
    for node in plan_nodes:
        if _coerce_str(node.get("step_id"), "") == step_id:
            return True
    return False


def _extract_json(content: str) -> dict[str, object] | None:
    """从LLM响应中提取JSON"""
    parsed_direct = _safe_load_json(content)
    if parsed_direct is not None:
        return parsed_direct

    # 尝试提取JSON块
    json_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", content)
    if json_match:
        parsed_block = _safe_load_json(json_match.group(1))
        if parsed_block is not None:
            return parsed_block

    # 尝试找到JSON对象
    brace_match = re.search(r"\{[\s\S]*\}", content)
    if brace_match:
        parsed_braces = _safe_load_json(brace_match.group(0))
        if parsed_braces is not None:
            return parsed_braces

    return None


def _safe_load_json(raw: str) -> dict[str, object] | None:
    try:
        parsed = json.loads(raw)  # pyright: ignore[reportAny]
    except Exception:
        return None

    if not isinstance(parsed, dict):
        return None

    normalized: dict[str, object] = {}
    parsed_dict: dict[object, object] = parsed  # pyright: ignore[reportUnknownVariableType]
    for key, value in parsed_dict.items():
        normalized[str(key)] = value
    return normalized


def _response_to_text(response: object) -> str:
    content = getattr(response, "content", None)
    if isinstance(content, str):
        return content
    return str(response)


def _resolve_schema_context(state: AgentState) -> str:
    """优先复用 state 中 schema_context，缺失时从 SchemaProvider 拉取。"""
    schema_context = state.get("schema_context")
    if isinstance(schema_context, str) and schema_context.strip():
        return schema_context

    workspace_id = state.get("workspace_id")
    try:
        try:
            from tools.schema_provider import get_schema_provider
        except ImportError:
            from ..tools.schema_provider import get_schema_provider  # type: ignore[reportMissingImports]

        provider = get_schema_provider(workspace_id)
        schema_text = provider.get_schema_text()
        if isinstance(schema_text, str) and schema_text.strip():
            return schema_text
    except Exception as exc:
        logger.warning("加载 schema_context 失败，降级为空对象: %s", exc)

    return "{}"


def _coerce_int(value: object, default: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _coerce_str(value: object, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return str(value)


def _coerce_plan_nodes(value: object) -> list[PlanNode]:
    if not isinstance(value, list):
        return []

    nodes_raw: list[object] = value  # pyright: ignore[reportUnknownVariableType]
    nodes: list[PlanNode] = []
    for node in nodes_raw:
        if isinstance(node, dict):
            node_map: dict[object, object] = node  # pyright: ignore[reportUnknownVariableType]
            node_dict: dict[str, object] = {}
            for k, v in node_map.items():
                node_dict[str(k)] = v
            nodes.append(node_dict)
    return nodes


def _coerce_observations(value: object) -> list[Observation]:
    if not isinstance(value, list):
        return []

    items_raw: list[object] = value  # pyright: ignore[reportUnknownVariableType]
    observations: list[Observation] = []
    for item in items_raw:
        if isinstance(item, dict):
            item_map: dict[object, object] = item  # pyright: ignore[reportUnknownVariableType]
            item_dict: dict[str, object] = {}
            for k, v in item_map.items():
                item_dict[str(k)] = v
            observations.append(item_dict)
    return observations


def _coerce_retry_counters(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}

    raw_map: dict[object, object] = value  # pyright: ignore[reportUnknownVariableType]
    counters: dict[str, int] = {}
    for raw_key, raw_val in raw_map.items():
        key = _coerce_str(raw_key, "")
        if not key:
            continue
        counters[key] = _coerce_int(raw_val, 0)
    return counters


def _coerce_step_results(value: object) -> dict[str, StepResult]:
    if not isinstance(value, dict):
        return {}

    raw_map: dict[object, object] = value  # pyright: ignore[reportUnknownVariableType]
    results: dict[str, StepResult] = {}
    for raw_key, raw_val in raw_map.items():
        key = _coerce_str(raw_key, "")
        if not key or not isinstance(raw_val, dict):
            continue
        result_map: dict[object, object] = raw_val  # pyright: ignore[reportUnknownVariableType]
        normalized: dict[str, object] = {}
        for k, v in result_map.items():
            normalized[str(k)] = v
        results[key] = normalized
    return results


def _coerce_execution_history(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    history_raw: list[object] = value  # pyright: ignore[reportUnknownVariableType]
    history: list[dict[str, object]] = []
    for item in history_raw:
        if not isinstance(item, dict):
            continue
        item_map: dict[object, object] = item  # pyright: ignore[reportUnknownVariableType]
        normalized: dict[str, object] = {}
        for k, v in item_map.items():
            normalized[str(k)] = v
        history.append(normalized)
    return history


__all__ = ["create_metric_loop_planner"]
