"""
测试日志记录功能
"""
import json
import os
import sys
import tempfile
from pathlib import Path

# 兼容从仓库根目录执行: python -m pytest text2sql/tests/...
TESTS_ROOT = Path(__file__).resolve().parents[1]
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from tools.logger import init_logger, log_trajectory, generate_trajectory_id


class TestLogger:
    """测试日志记录器"""
    
    def test_log_metric_trajectory(self):
        """测试 METRIC 路径的日志记录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            init_logger(tmpdir)
            
            log_trajectory(
                trajectory_id="test-metric-001",
                user_query="计算基础设施得分",
                intent_type="metric_query",
                query_plan={"plan_nodes": [{"step_id": "s1"}]},
                analysis_code=None,  # 新的循环不使用
                analysis_result=None,  # 新的循环不使用
                analysis_error=None,
                verification_passed=None,
                verification_feedback=None,
                metric_plan_nodes=[{"step_id": "s1", "intent": "aggregate"}],
                execution_history=[{"step_id": "s1", "status": "success"}],
                step_results={
                    "s1": {
                        "step_id": "s1",
                        "generated_sql": "SELECT province, AVG(score) as avg_score FROM schools GROUP BY province",
                        "status": "success",
                        "row_count": 31,
                        "execution_time_ms": 150,
                        "is_final_step": True,
                    }
                },
                loop_status="completed",
                final_response="基础设施得分是85.5",
                workspace_id="test-ws"
            )
            
            # 读取日志文件
            from datetime import datetime
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(tmpdir, f"trajectory_{date_str}.jsonl")
            
            assert os.path.exists(log_file), "日志文件应该被创建"
            
            with open(log_file, 'r', encoding='utf-8') as f:
                record = json.loads(f.readline())
            
            assert record["trajectory_id"] == "test-metric-001"
            assert record["user_query"] == "计算基础设施得分"
            assert record["intent_type"] == "metric_query"
            assert record["execution_path"] == "metric"
            
            # 检查 METRIC 路径字段 - 新的迭代式循环
            assert record["metric_path"] is not None
            assert record["metric_path"]["metric_plan_nodes"] == [{"step_id": "s1", "intent": "aggregate"}]
            assert record["metric_path"]["execution_history"] == [{"step_id": "s1", "status": "success"}]
            assert record["metric_path"]["step_results"]["s1"]["row_count"] == 31
            assert record["metric_path"]["step_results"]["s1"]["generated_sql"] == "SELECT province, AVG(score) as avg_score FROM schools GROUP BY province"
            assert record["metric_path"]["loop_status"] == "completed"
            # 旧的字段应该为 None
            assert record["metric_path"]["analysis_code"] is None
            assert record["metric_path"]["analysis_result"] is None
            
            # 检查 sql_snapshots 字段
            assert "sql_snapshots" in record["metric_path"]
            snapshots = record["metric_path"]["sql_snapshots"]
            assert len(snapshots) == 1
            assert snapshots[0]["step_id"] == "s1"
            assert "SELECT province" in snapshots[0]["sql"]
            assert snapshots[0]["row_count"] == 31
            assert snapshots[0]["status"] == "success"
            
            # VALUE 路径应该为空
            assert record["value_path"] is None
    
    def test_log_metric_multiple_steps(self):
        """测试多步骤METRIC查询的日志记录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            init_logger(tmpdir)
            
            step_results = {
                "s1": {
                    "step_id": "s1",
                    "generated_sql": "CREATE TEMPORARY TABLE temp_s1 AS SELECT province, score FROM schools",
                    "status": "success",
                    "row_count": 1000,
                    "execution_time_ms": 200,
                    "is_final_step": False,
                },
                "s2": {
                    "step_id": "s2",
                    "generated_sql": "SELECT province, AVG(score) as avg_score FROM temp_s1 GROUP BY province",
                    "status": "success",
                    "row_count": 31,
                    "execution_time_ms": 100,
                    "is_final_step": True,
                }
            }
            
            log_trajectory(
                trajectory_id="test-metric-multi",
                user_query="计算各省平均分",
                intent_type="metric_query",
                metric_plan_nodes=[
                    {"step_id": "s1", "intent": "filter"},
                    {"step_id": "s2", "intent": "aggregate"}
                ],
                execution_history=[
                    {"step_id": "s1", "status": "success"},
                    {"step_id": "s2", "status": "success"}
                ],
                step_results=step_results,
                loop_status="completed",
                final_response="各省平均分已计算完成",
            )
            
            from datetime import datetime
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(tmpdir, f"trajectory_{date_str}.jsonl")
            
            with open(log_file, 'r', encoding='utf-8') as f:
                record = json.loads(f.readline())
            
            # 检查 sql_snapshots 包含所有步骤
            snapshots = record["metric_path"]["sql_snapshots"]
            assert len(snapshots) == 2
            # 应该按步骤ID排序
            assert snapshots[0]["step_id"] == "s1"
            assert snapshots[1]["step_id"] == "s2"
            # 检查SQL内容
            assert "CREATE TEMPORARY TABLE" in snapshots[0]["sql"]
            assert "SELECT province" in snapshots[1]["sql"]
            # 检查行数
            assert snapshots[0]["row_count"] == 1000
            assert snapshots[1]["row_count"] == 31
    
    def test_log_value_trajectory(self):
        """测试 VALUE 路径的日志记录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            init_logger(tmpdir)
            
            log_trajectory(
                trajectory_id="test-value-001",
                user_query="查询北京学校数量",
                intent_type="value_query",
                query_plan={"target_fields": ["count"]},
                generated_sql="SELECT COUNT(*) FROM schools WHERE province LIKE '%北京%'",
                execution_result=[{"count": 150}],
                execution_error=None,
                sql_reflection="SQL执行成功",
                final_response="北京有150所学校",
                workspace_id="test-ws"
            )
            
            from datetime import datetime
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(tmpdir, f"trajectory_{date_str}.jsonl")
            
            with open(log_file, 'r', encoding='utf-8') as f:
                record = json.loads(f.readline())
            
            assert record["intent_type"] == "value_query"
            assert record["execution_path"] == "value"
            
            # 检查 VALUE 路径字段
            assert record["value_path"] is not None
            assert "SELECT COUNT(*)" in record["value_path"]["generated_sql"]
            assert record["value_path"]["execution_result"][0]["count"] == 150
            assert record["value_path"]["sql_reflection"] == "SQL执行成功"
            
            # METRIC 路径应该为空
            assert record["metric_path"] is None
    
    def test_log_metric_with_enum_intent_type(self):
        """测试 IntentType.METRIC_QUERY 枚举格式的日志记录"""
        with tempfile.TemporaryDirectory() as tmpdir:
            init_logger(tmpdir)
            
            # 测试枚举字符串格式
            log_trajectory(
                trajectory_id="test-enum-metric",
                user_query="计算基础设施得分",
                intent_type="IntentType.METRIC_QUERY",  # 枚举格式
                step_results={
                    "s1": {
                        "step_id": "s1",
                        "generated_sql": "SELECT * FROM schools",
                        "status": "success",
                        "row_count": 100,
                    }
                },
                execution_history=[{"step_id": "s1", "status": "success"}],
                loop_status="completed",
            )
            
            from datetime import datetime
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(tmpdir, f"trajectory_{date_str}.jsonl")
            
            with open(log_file, 'r', encoding='utf-8') as f:
                record = json.loads(f.readline())
            
            # 关键验证：即使 intent_type 是 "IntentType.METRIC_QUERY"，也应该识别为 metric 路径
            assert record["intent_type"] == "IntentType.METRIC_QUERY"
            assert record["execution_path"] == "metric"
            assert record["metric_path"] is not None
            assert record["value_path"] is None
            assert len(record["metric_path"]["sql_snapshots"]) == 1
    
    def test_log_without_intent_type(self):
        """测试没有意图类型时的默认行为"""
        with tempfile.TemporaryDirectory() as tmpdir:
            init_logger(tmpdir)
            
            log_trajectory(
                trajectory_id="test-default-001",
                user_query="查询数据",
                generated_sql="SELECT * FROM table",
                execution_result=[{"id": 1}],
            )
            
            from datetime import datetime
            date_str = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(tmpdir, f"trajectory_{date_str}.jsonl")
            
            with open(log_file, 'r', encoding='utf-8') as f:
                record = json.loads(f.readline())
            
            # 默认应该识别为 VALUE 路径
            assert record["execution_path"] == "value"
            assert record["value_path"] is not None
