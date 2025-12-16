"""
Refactor Verification Script
"""
import os
import sys
from unittest.mock import MagicMock

# 模拟环境
os.environ["LLM_API_KEY"] = "test"

# Hack: 模拟不可用的 langchain_openai 以强制使用 SimpleLLMClient (如果需要)
# 但这里我们希望测试实际流程，所以尽可能使用配置中的 LLM

from config import config
from graph import create_graph

class MockLLM:
    def invoke(self, prompt: str):
        print(f"\n[LLM Prompt Preview]: {str(prompt)[:200]}...\n")
        class Resp:
            content = '{"intent_type": "metric_query", "analysis": "Test Analysis"}'
        return Resp()

def test_workflow():
    print("Testing Workflow with Mock LLM...")
    
    # 1. 创建 Graph (不带 Embedding)
    app = create_graph(
        llm_client=MockLLM(),
        embedding_client=None,
        db_connection=None
    )
    
    # 2. 测试输入
    initial_state = {
        "user_query": "查询北京市的学校平均得分",
        "messages": [],
        "clarification_count": 0
    }
    
    print("Invoking graph...")
    try:
        result = app.invoke(initial_state)
        print("Graph invocation successful!")
        
        # 3. 验证 Prompt 中是否包含指标
        prompt = result.get("assembled_prompt", "")
        if "完整指标体系" in prompt or "指标解释" in prompt:
             print("SUCCESS: Full metrics context found in prompt.")
        else:
             print("WARNING: Metrics context might be missing.")
             print(f"Prompt content: {prompt[:500]}")
             
    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_workflow()
