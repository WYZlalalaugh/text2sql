"""
查询规划智能体 - 使用 LLM 生成结构化查询计划 (Reasoning Plan)
"""
import json
import logging
from typing import Protocol, cast

from state import AgentState, IntentType
from tools.schema_provider import get_schema_provider

try:
    from config import config
except ImportError:
    from ..config import config

from prompts.query_planner_prompt import (
    SIMPLE_QUERY_PLANNER_PROMPT_TEMPLATE,
    SIMPLE_QUERY_PLANNER_SYSTEM_PROMPT,
    build_iterative_metric_planner_prompt,
)

logger = logging.getLogger(__name__)


class QueryPlannerLLM(Protocol):
    def invoke(self, prompt: str) -> object:
        ...


class LLMResponseWithContent(Protocol):
    content: str


def create_query_planner(llm_client: QueryPlannerLLM):
    """
    创建查询规划节点

    Args:
        llm_client: LLM 客户端
    """

    def query_planner_node(state: AgentState) -> dict[str, object]:
        """查询规划节点 - 根据查询类型使用不同的提示词"""
        config.refresh_feature_flags()
        user_query = state.get("user_query", "")
        refined_intent = state.get("refined_intent") or user_query
        intent_type = state.get("intent_type", IntentType.SIMPLE_QUERY)
        is_metric_query_intent = _is_metric_query_intent(intent_type)
        schema_provider = get_schema_provider(state.get("workspace_id"))

        schema = _safe_prompt_context_text(
            schema_provider.get_schema_text(),
            context_name="schema",
        )

        if is_metric_query_intent:
            # 优先使用从 clarification 步骤提取的指标上下文
            metrics_context = state.get("metrics_context")
            if metrics_context and isinstance(metrics_context, list) and len(metrics_context) > 0:
                # 将 [{"一级指标": "xxx", "二级指标": "yyy"}, ...] 格式转换为 JSON 字符串
                import json
                full_metrics = json.dumps({"selected_indicators": metrics_context}, ensure_ascii=False)
            else:
                full_metrics = _safe_prompt_context_text(
                    cast(str, cast(object, schema_provider.get_metrics_text())),  # pyright: ignore[reportAny]
                    context_name="metrics",
                )
            prompt = build_iterative_metric_planner_prompt(
                metrics=full_metrics,
                schema=schema,
                query=refined_intent,
            )
        else:
            prompt = SIMPLE_QUERY_PLANNER_PROMPT_TEMPLATE.format(
                system_prompt=SIMPLE_QUERY_PLANNER_SYSTEM_PROMPT,
                schema=schema,
                query=refined_intent,
            )

        response = llm_client.invoke(prompt)
        if hasattr(response, "content"):
            response_text = cast(LLMResponseWithContent, response).content
        else:
            response_text = str(response)

        query_plan, validation_error = _normalize_and_validate_plan(
            _parse_plan_json(response_text),
            intent_type,
        )

        if validation_error:
            logger.warning("Query planner 首次输出无效，尝试重试...")
            retry_suffix = (
                "\n\n⚠️ 你上次的输出未能被正确解析为 JSON，或缺少必填字段。"
                f" 问题: {validation_error}。请严格按照输出格式要求重新输出 JSON。"
            )
            retry_response = llm_client.invoke(prompt + retry_suffix)
            if hasattr(retry_response, "content"):
                retry_text = cast(LLMResponseWithContent, retry_response).content
            else:
                retry_text = str(retry_response)

            query_plan, validation_error = _normalize_and_validate_plan(
                _parse_plan_json(retry_text),
                intent_type,
            )
            if validation_error:
                logger.error("Query planner 重试后仍然无效，标记为规划失败")
                return _build_planning_failure_state(validation_error)

        reasoning_plan_text = _extract_reasoning_plan_text(query_plan, intent_type)
        selected_metrics = _extract_selected_metrics(query_plan, intent_type)
        target_fields = _extract_target_fields(query_plan, intent_type)

        return {
            "query_plan": query_plan,
            "reasoning_plan": reasoning_plan_text,
            "selected_metrics": selected_metrics,
            "target_fields": target_fields,
            "current_node": "query_planner",
        }

    return query_planner_node


def _safe_prompt_context_text(value: object, *, context_name: str) -> str:
    if isinstance(value, str):
        text = value.strip()
        if text:
            return text
    logger.warning(
        "Query planner received empty %s context; injecting '{}' placeholder to avoid blank prompt blocks",
        context_name,
    )
    return "{}"


def _parse_plan_json(text: str) -> dict[str, object]:
    """从 LLM 文本响应中提取 JSON 对象"""
    try:
        json_start = text.find("{")
        json_end = text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            parsed = cast(object, json.loads(text[json_start:json_end]))
            if isinstance(parsed, dict):
                return cast(dict[str, object], parsed)
        return {}
    except json.JSONDecodeError:
        return {}


def _normalize_and_validate_plan(
    plan: dict[str, object],
    intent_type: IntentType,
) -> tuple[dict[str, object], str]:
    validation_error = _get_plan_validation_error(plan, intent_type)
    if validation_error:
        return plan, validation_error
    return plan, ""


def _build_planning_failure_state(validation_error: str) -> dict[str, object]:
    return {
        "query_plan": {},
        "reasoning_plan": "",
        "selected_metrics": [],
        "target_fields": [],
        "planning_error": f"查询规划失败：{validation_error}",
        "current_node": "query_planner",
    }


def _is_plan_valid(plan: dict[str, object], intent_type: IntentType) -> bool:
    """
    检查查询计划是否包含足够信息供下游节点使用

    METRIC_QUERY 需要: plan_nodes 或 reasoning/reasoning_steps
    VALUE_QUERY 需要: target_fields 或 reasoning/reasoning_steps
    """
    if not plan:
        return False

    # 支持新的迭代式指标规划格式 (plan_nodes + reasoning)
    # 也兼容旧格式 (reasoning_steps + selected_metrics/target_fields)
    has_reasoning = bool(plan.get("reasoning_steps") or plan.get("reasoning"))
    has_plan_nodes = bool(plan.get("plan_nodes"))

    if _is_metric_query_intent(intent_type):
        has_required_filters = _metric_filter_nodes_have_filters(plan)
        has_terminal_step = _metric_has_terminal_output_step(plan)
        return (has_reasoning or has_plan_nodes or bool(plan.get("selected_metrics"))) and has_required_filters and has_terminal_step
    return has_reasoning or bool(plan.get("target_fields"))


def _get_plan_validation_error(
    plan: dict[str, object],
    intent_type: IntentType,
) -> str:
    if _is_plan_valid(plan, intent_type):
        return ""
    if _is_metric_query_intent(intent_type) and not _metric_filter_nodes_have_filters(plan):
        return "METRIC_QUERY 的 filter 步骤缺少 filters 条件"
    if _is_metric_query_intent(intent_type) and not _metric_has_terminal_output_step(plan):
        return "METRIC_QUERY 缺少终局汇总步骤（最后一步必须是 aggregate/derive 且 expected_outputs 非空）"
    return "LLM 未能生成满足当前查询类型要求的计划字段"


def _metric_filter_nodes_have_filters(plan: dict[str, object]) -> bool:
    plan_nodes_obj = plan.get("plan_nodes")
    if not isinstance(plan_nodes_obj, list):
        return True

    plan_nodes = cast(list[object], plan_nodes_obj)
    for node_obj in plan_nodes:
        if not isinstance(node_obj, dict):
            continue
        node = cast(dict[str, object], node_obj)
        intent_type = node.get("intent_type")
        if intent_type != "filter":
            continue

        filters_obj = node.get("filters")
        if not isinstance(filters_obj, list):
            return False
        filters = cast(list[object], filters_obj)
        if len(filters) == 0:
            return False
    return True


def _metric_has_terminal_output_step(plan: dict[str, object]) -> bool:
    plan_nodes_obj = plan.get("plan_nodes")
    if not isinstance(plan_nodes_obj, list):
        return True
    if len(cast(list[object], plan_nodes_obj)) == 0:
        return True

    node_dicts: list[dict[str, object]] = []
    plan_nodes = cast(list[object], plan_nodes_obj)
    for node_obj in plan_nodes:
        if isinstance(node_obj, dict):
            node_dicts.append(cast(dict[str, object], node_obj))

    if len(node_dicts) == 0:
        return True

    last_node = node_dicts[-1]
    last_intent = last_node.get("intent_type")
    if last_intent not in {"aggregate", "derive"}:
        return False

    expected_outputs_obj = last_node.get("expected_outputs")
    if not isinstance(expected_outputs_obj, list):
        return False
    expected_outputs = cast(list[object], expected_outputs_obj)
    if len(expected_outputs) == 0:
        return False

    return True


def _extract_reasoning_plan_text(
    query_plan: dict[str, object],
    intent_type: IntentType,
) -> str:
    _ = intent_type
    reasoning_steps = _as_str_list(query_plan.get("reasoning_steps"))
    return "\n".join(reasoning_steps) if reasoning_steps else ""


def _extract_selected_metrics(
    query_plan: dict[str, object],
    intent_type: IntentType,
) -> list[str]:
    _ = intent_type
    return _as_str_list(query_plan.get("selected_metrics"))


def _extract_target_fields(
    query_plan: dict[str, object],
    intent_type: IntentType,
) -> list[str]:
    _ = intent_type
    return _as_str_list(query_plan.get("target_fields"))


def _is_metric_query_intent(intent_type: object) -> bool:
    return intent_type in {IntentType.METRIC_QUERY, IntentType.METRIC_QUERY.value}


def _as_str_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []

    normalized: list[str] = []
    for item in cast(list[object], value):
        if isinstance(item, str):
            normalized.append(item)
    return normalized
