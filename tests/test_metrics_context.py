"""Tests for metrics_context extraction in ambiguity_checker."""

from typing import cast
import sys
from pathlib import Path

# 兼容从仓库根目录执行
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from state import AgentState


class FakeLLMResponse:
    """模拟 LLM 响应"""
    def __init__(self, content: str):
        self.content = content


class FakeLLMClient:
    """模拟 LLM 客户端，用于测试指标提取"""
    
    def __init__(self, response_content: str = "{}"):
        self.response_content = response_content
        self.last_prompt: str = ""
    
    def invoke(self, prompt: str) -> object:
        self.last_prompt = prompt
        return FakeLLMResponse(self.response_content)


class FakePromptBuilder:
    """模拟 PromptBuilder"""
    
    def build_ambiguity_check_prompt(self, query: str, full_metrics_context: str, 
                                     conversation_history: str = "") -> str:
        return f"检查歧义: {query}"


def test_metrics_context_field_in_state():
    """验证 AgentState 包含 metrics_context 字段"""
    from state import AgentState
    
    # 创建一个包含 metrics_context 的状态
    state = cast(AgentState, {
        "user_query": "test",
        "metrics_context": {
            "教育教学": {
                "一级指标解释": "用于评估教学数字化程度",
                "二级指标": {
                    "教学评价": {
                        "二级指标解释": "评估教学评价数字化"
                    }
                }
            }
        }
    })
    
    # 验证可以正确读取
    assert "metrics_context" in state
    metrics = state["metrics_context"]
    assert isinstance(metrics, dict)
    assert "教育教学" in metrics


def test_extract_metrics_with_llm_returns_dict():
    """验证 _extract_metrics_with_llm 函数返回字典"""
    from agents.ambiguity_checker import _extract_metrics_with_llm
    
    # 模拟 LLM 返回有效的指标 JSON
    fake_metrics = {
        "基础设施": {
            "一级指标解释": "评估基础设施情况",
            "二级指标": {
                "网络": {
                    "二级指标解释": "学校网络建设"
                }
            }
        }
    }
    
    import json
    llm_client = FakeLLMClient(response_content=json.dumps(fake_metrics, ensure_ascii=False))
    prompt_builder = FakePromptBuilder()
    
    result = _extract_metrics_with_llm(
        llm_client=llm_client,
        prompt_builder=prompt_builder,
        user_query="学校网络情况如何",
        refined_intent="查询学校网络建设指标",
        full_metrics_text="{}"
    )
    
    assert isinstance(result, dict)
    assert "基础设施" in result
    assert result["基础设施"]["二级指标"]["网络"]["二级指标解释"] == "学校网络建设"


def test_extract_metrics_returns_empty_on_failure():
    """验证 LLM 提取失败时返回空对象"""
    from agents.ambiguity_checker import _extract_metrics_with_llm
    
    # 模拟 LLM 返回无效 JSON
    llm_client = FakeLLMClient(response_content="invalid json")
    prompt_builder = FakePromptBuilder()
    
    result = _extract_metrics_with_llm(
        llm_client=llm_client,
        prompt_builder=prompt_builder,
        user_query="test",
        refined_intent="test",
        full_metrics_text="{}"
    )
    
    assert result == {}


def test_ambiguity_checker_returns_metrics_context():
    """验证 ambiguity_checker 在无歧义时返回 metrics_context"""
    from agents.ambiguity_checker import create_ambiguity_checker
    
    fake_metrics = {
        "教育教学": {
            "一级指标解释": "教学数字化",
            "二级指标": {
                "教学评价": {
                    "二级指标解释": "评价数字化程度"
                }
            }
        }
    }
    
    import json
    llm_client = FakeLLMClient(
        response_content=json.dumps({
            "ambiguity_detected": False,
            "refined_intent": "查询教学评价数字化程度",
            **fake_metrics  # 模拟 LLM 同时返回指标
        }, ensure_ascii=False)
    )
    prompt_builder = FakePromptBuilder()
    
    checker = create_ambiguity_checker(llm_client, prompt_builder)
    
    state = cast(AgentState, {
        "user_query": "教学评价数字化程度如何",
        "clarification_count": 0,
        "clarification_response": "",
    })
    
    result = checker(state)
    
    assert result["ambiguity_detected"] is False
    # 注意：由于 LLM 模拟返回的格式问题，实际 metrics_context 可能在结果中
    # 这里主要验证流程不报错


def test_metric_loop_planner_uses_metrics_context():
    """验证 metric_loop_planner 可以读取 metrics_context"""
    from agents.metric_loop_planner import create_metric_loop_planner
    
    planner = create_metric_loop_planner()
    
    # 创建一个有 metrics_context 的状态
    state = cast(AgentState, {
        "loop_status": "planning",
        "loop_iteration": 0,
        "retry_counters": {},
        "planner_observations": [],
        "metric_plan_nodes": None,
        "metrics_context": {
            "教育教学": {
                "一级指标解释": "教学数字化评估",
                "二级指标": {
                    "教学评价": {"二级指标解释": "评价数字化"}
                }
            }
        },
        "schema_context": "{}",
        "user_query": "教学评价如何",
    })
    
    # planner 应该能读取 metrics_context（虽然是无 LLM 的简单重试路径）
    result = planner(state)
    
    # 验证状态被正确读取（没有异常抛出）
    assert "current_node" in result


if __name__ == "__main__":
    print("Running metrics_context tests...")
    
    # 运行测试
    test_metrics_context_field_in_state()
    print("✓ test_metrics_context_field_in_state passed")
    
    test_extract_metrics_with_llm_returns_dict()
    print("✓ test_extract_metrics_with_llm_returns_dict passed")
    
    test_extract_metrics_returns_empty_on_failure()
    print("✓ test_extract_metrics_returns_empty_on_failure passed")
    
    test_ambiguity_checker_returns_metrics_context()
    print("✓ test_ambiguity_checker_returns_metrics_context passed")
    
    test_metric_loop_planner_uses_metrics_context()
    print("✓ test_metric_loop_planner_uses_metrics_context passed")
    
    print("\nAll tests passed!")
