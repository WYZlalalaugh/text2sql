"""
分析最新日志并测试指标类查询
"""
import json
from pathlib import Path

# 读取最新日志
log_file = Path("text2sql/logs/trajectory_20260401.jsonl")
if log_file.exists():
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    if lines:
        # 解析最后一条日志
        last_entry = json.loads(lines[-1])
        
        print("=" * 80)
        print("最新日志分析")
        print("=" * 80)
        print(f"\n查询: {last_entry.get('user_query')}")
        print(f"意图类型: {last_entry.get('intent_type')}")
        print(f"执行路径: {last_entry.get('execution_path')}")
        print(f"\n--- METRIC 路径详情 ---")
        
        metric_path = last_entry.get('metric_path', {})
        if metric_path:
            print(f"\n计划节点数: {len(metric_path.get('metric_plan_nodes', []))}")
            print(f"执行历史数: {len(metric_path.get('execution_history', []))}")
            print(f"步骤结果数: {len(metric_path.get('step_results', {}))}")
            print(f"循环状态: {metric_path.get('loop_status')}")
            
            # 显示 SQL 快照
            sql_snapshots = metric_path.get('sql_snapshots', [])
            if sql_snapshots:
                print(f"\n--- SQL 执行快照 ({len(sql_snapshots)} 个步骤) ---")
                for snapshot in sql_snapshots:
                    print(f"\n  步骤 {snapshot.get('step_id')}:")
                    print(f"    SQL: {snapshot.get('sql', 'N/A')[:100]}...")
                    print(f"    状态: {snapshot.get('status')}")
                    print(f"    行数: {snapshot.get('row_count')}")
                    print(f"    耗时: {snapshot.get('execution_time_ms')}ms")
            
            # 显示执行历史（失败信息）
            execution_history = metric_path.get('execution_history', [])
            if execution_history:
                print(f"\n--- 执行历史 ---")
                for record in execution_history:
                    print(f"\n  步骤 {record.get('step_id')}:")
                    print(f"    状态: {record.get('status')}")
                    if record.get('error'):
                        print(f"    错误: {record.get('error')}")
                    if record.get('sql'):
                        print(f"    SQL: {record.get('sql')[:100]}...")
        
        print(f"\n--- 系统响应 ---")
        print(f"{last_entry.get('final_response', 'N/A')[:300]}...")
        
        print("\n" + "=" * 80)
