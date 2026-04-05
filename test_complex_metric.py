"""
复杂Metric查询测试 - 自动澄清并执行
测试查询：对于数字化经费投入（财力保障）力度较大的前20%省份，其教学评价的数字化程度是否也相应领先？
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime
from config import config
from runtime_bootstrap import create_runtime_graph
from state import AgentState


def test_complex_metric_with_auto_clarification():
    """
    测试复杂Metric查询，自动处理澄清
    """
    print("=" * 80)
    print("Complex Metric Query Test")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Model: {config.llm.model_name}")
    print("-" * 80)

    # Init
    print("\n[1/2] Initializing LangGraph...")
    app, _, _ = create_runtime_graph(
        llm_client=None,
        embedding_client=None,
        db_connection=None,
        enable_embedding_in_graph=False,
    )
    print("[OK] Initialized")

    # 复杂查询
    query = "对于数字化经费投入（财力保障）力度较大的前20%省份，其教学评价的数字化程度是否也相应领先？"
    print(f"\n[2/2] Query: {query}")
    print("-" * 80)

    # 初始状态
    state: AgentState = {
        "user_query": query,
        "messages": [],
        "clarification_count": 0,
    }

    thread_id = f"complex_metric_{datetime.now().strftime('%H%M%S')}"
    thread_config = {"configurable": {"thread_id": thread_id}}

    # 执行并跟踪
    step_count = 0
    clarification_count = 0
    max_clarifications = 3

    print("\n>>> Starting execution...\n")

    while clarification_count < max_clarifications:
        step_count += 1

        # 执行一步
        events = list(app.stream(state, stream_mode="updates", config=thread_config))

        for event in events:
            for node_name, node_output in event.items():
                # 打印节点信息
                print(f"\n[Step {step_count}] {node_name}")

                if isinstance(node_output, dict):
                    # 意图分类
                    if "intent_type" in node_output:
                        print(f"    Intent: {node_output['intent_type']}")

                    # 歧义检测
                    if node_output.get("ambiguity_detected"):
                        clarification_q = node_output.get("clarification_question", '')
                        print(f"\n    [CLARIFICATION NEEDED]")
                        print(f"    Question: {clarification_q[:300]}...")

                        # 自动提供澄清回复
                        clarification_reply = _auto_clarify(clarification_q)
                        print(f"\n    [AUTO REPLY] {clarification_reply}")

                        # 更新状态
                        state = dict(state)
                        state["clarification_response"] = clarification_reply
                        state["messages"] = state.get("messages", []) + [
                            {"role": "assistant", "content": clarification_q},
                            {"role": "user", "content": clarification_reply}
                        ]
                        clarification_count += 1
                        break

                    # Metric loop 信息
                    if node_name == "metric_loop_planner":
                        if "loop_status" in node_output:
                            print(f"    Loop status: {node_output['loop_status']}")
                        if "loop_iteration" in node_output:
                            print(f"    Loop iteration: {node_output['loop_iteration']}")
                        if "current_step_id" in node_output:
                            print(f"    Current step: {node_output['current_step_id']}")

                    # SQL 生成
                    if node_name == "metric_sql_generator":
                        if node_output.get("generated_sql"):
                            sql_preview = node_output['generated_sql'][:150].replace('\n', ' ')
                            print(f"    SQL: {sql_preview}...")

                    # 执行结果
                    if node_name == "metric_executor":
                        if node_output.get("execution_error"):
                            error = node_output['execution_error'][:150]
                            print(f"    Error: {error}...")
                        if node_output.get("execution_history"):
                            history = node_output['execution_history']
                            print(f"    Execution history: {len(history)} records")
                            if history:
                                last = history[-1]
                                print(f"      Last: Step {last.get('step_id')} - {last.get('status')}")

                    # 最终响应
                    if node_name == "response_generator" and node_output.get("final_response"):
                        print(f"\n    [FINAL RESPONSE]")
                        response = node_output['final_response']
                        print(f"    {response[:500]}...")
                        print(f"\n{'='*80}")
                        print("Execution Complete")
                        print("="*80)
                        _print_summary(app, state, thread_config)
                        return

        # 检查是否完成
        current_state = app.invoke(state, config=thread_config)
        if current_state.get("final_response"):
            break

        if step_count > 100:
            print("\n[!] Too many steps, breaking...")
            break

    # Final summary
    _print_summary(app, state, thread_config)


def _auto_clarify(question: str) -> str:
    """
    根据澄清问题自动生成回复
    """
    q_lower = question.lower()

    # 根据问题内容自动选择
    if "数字化经费投入" in question or "财力保障" in question:
        return "A. 使用教育信息化经费投入指标"

    if "前20%" in question or "省份" in question:
        return "A. 按省份分组，取经费投入前20%的省份"

    if "教学评价" in question or "数字化程度" in question:
        return "A. 教学评价数字化程度使用在线评价系统覆盖率指标"

    if "时间范围" in question or "年份" in question:
        return "A. 分析2021-2023年最近三年数据"

    if "对比方式" in question or "分析" in question:
        return "A. 对比前20%省份与其他省份的教学评价数字化程度差异"

    # 默认回复
    return "A. 使用默认选项，分析最近三年全省数据"


def _print_summary(app, state, thread_config):
    """打印执行摘要"""
    final_state = app.invoke(state, config=thread_config)

    print("\n" + "=" * 80)
    print("Execution Summary")
    print("=" * 80)

    print(f"\nFinal State:")
    print(f"  Intent: {final_state.get('intent_type')}")
    print(f"  Loop status: {final_state.get('loop_status')}")
    print(f"  Loop iteration: {final_state.get('loop_iteration')}")
    print(f"  Planning error: {final_state.get('planning_error', 'None')}")

    if final_state.get('metric_plan_nodes'):
        nodes = final_state['metric_plan_nodes']
        print(f"\n  Metric Plan ({len(nodes)} nodes):")
        for i, node in enumerate(nodes[:8], 1):
            print(f"    {i}. {node.get('step_id', 'N/A')}: {node.get('intent_type', 'N/A')} - "
                  f"{node.get('description', 'N/A')[:50]}...")

    if final_state.get('execution_history'):
        history = final_state['execution_history']
        print(f"\n  Execution History ({len(history)} records):")

        # 分析重试
        step_counts = {}
        for record in history:
            sid = record.get('step_id')
            if sid:
                step_counts[sid] = step_counts.get(sid, 0) + 1

        retries = {s: c for s, c in step_counts.items() if c > 1}

        for i, record in enumerate(history[:15], 1):
            step_id = record.get('step_id', 'N/A')
            status = record.get('status', 'unknown')
            error = record.get('error', '')
            if error:
                print(f"    {i}. Step {step_id}: {status}")
                print(f"       Error: {error[:80]}...")
            else:
                print(f"    {i}. Step {step_id}: {status}")

        if retries:
            print(f"\n  [RETRIES DETECTED] {retries}")
        else:
            print(f"\n  [INFO] All steps passed on first attempt")

    if final_state.get('final_response'):
        print(f"\n  Final Response:")
        print(f"  {final_state['final_response'][:800]}...")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    test_complex_metric_with_auto_clarification()
