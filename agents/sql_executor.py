"""
SQL 执行节点 - 连接 MySQL 执行 SQL
支持两种模式:
1. 普通模式: 直接返回结果 (用于 VALUE_QUERY)
2. 流式模式: 流式写入 CSV 文件 (用于 METRIC_QUERY, 防止内存溢出)
"""
from typing import Dict, Any, List, Optional
import json
import csv
import os
import uuid

from state import AgentState, IntentType
from config import config
from datetime import datetime, date
from decimal import Decimal

# 临时文件目录
TEMP_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp")


def ensure_temp_dir():
    """确保临时目录存在"""
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)


def create_sql_executor(db_connection=None):
    """
    创建 SQL 执行节点
    
    Args:
        db_connection: 可选的数据库连接对象
    """
    
    def sql_executor_node(state: AgentState) -> Dict[str, Any]:
        """SQL 执行节点 - 根据意图类型选择执行模式"""
        generated_sql = state.get("generated_sql", "")
        intent_type = state.get("intent_type")
        
        if not generated_sql:
            return {
                "execution_result": None,
                "execution_error": "没有 SQL 可执行",
                "current_node": "sql_executor"
            }
        
        # 安全检查
        if not is_safe_sql(generated_sql):
            return {
                "execution_result": None,
                "execution_error": "SQL 安全检查未通过",
                "current_node": "sql_executor"
            }
        
        # 根据意图类型选择执行模式
        is_metric_query = (intent_type == IntentType.METRIC_QUERY or 
                          intent_type == "metric_query")
        
        try:
            if is_metric_query:
                # 指标查询: 使用流式写入 CSV, 避免内存溢出
                return _execute_streaming_to_csv(generated_sql, db_connection)
            else:
                # 普通查询: 直接返回结果
                return _execute_normal(generated_sql, db_connection)
                
        except Exception as e:
            # 结构化错误信息，便于纠错
            error_message = str(e)
            error_type = type(e).__name__
            
            return {
                "execution_result": None,
                "execution_observation": f"ERROR: [{error_type}] {error_message}",
                "execution_error": f"[{error_type}] {error_message}",
                "current_node": "sql_executor"
            }
    
    return sql_executor_node


def _execute_normal(sql: str, db_connection=None) -> Dict[str, Any]:
    """普通模式执行 SQL, 返回完整结果"""
    if db_connection is not None:
        result = execute_with_connection(db_connection, sql)
    else:
        result = execute_sql(sql)
    
    # 生成观测结果（用于 ReAct 反思）
    observation = format_observation(result)
    
    return {
        "execution_result": result,
        "execution_observation": observation,
        "execution_error": None,
        "current_node": "sql_executor"
    }


def _execute_streaming_to_csv(sql: str, db_connection=None) -> Dict[str, Any]:
    """
    流式执行 SQL 并写入 CSV 文件
    
    使用 fetchmany 分批获取 + csv.DictWriter 流式写入
    优点: O(1) 内存占用, 可处理 GB 级数据
    """
    import pymysql
    
    ensure_temp_dir()
    
    # 生成唯一文件名
    query_id = str(uuid.uuid4())[:8]
    file_path = os.path.join(TEMP_DIR, f"query_{query_id}.csv")
    
    connection = None
    cursor = None
    row_count = 0
    columns = []
    
    try:
        # 建立连接
        if db_connection is not None:
            connection = db_connection
            cursor = connection.cursor()
            owns_connection = False
        else:
            connection = pymysql.connect(
                host=config.database.host,
                port=config.database.port,
                user=config.database.user,
                password=config.database.password,
                database=config.database.database,
                charset=config.database.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = connection.cursor()
            owns_connection = True
        
        # 执行 SQL
        cursor.execute(sql)
        
        # 获取列名
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
        
        if not columns:
            # 无结果集 (可能是非 SELECT 语句或空结果)
            return {
                "execution_result": [],
                "data_file_path": None,
                "execution_observation": "Observation: 执行成功，但未返回数据列。",
                "execution_error": None,
                "current_node": "sql_executor"
            }
        
        # 流式写入 CSV
        batch_size = 2000  # 每批获取 2000 行
        sample_rows = []  # 保存前 5 行用于观测
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            
            while True:
                # 分批获取
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                # 清洗并写入
                for row in batch:
                    sanitized_row = _sanitize_row(row)
                    writer.writerow(sanitized_row)
                    row_count += 1
                    
                    # 保存样本
                    if len(sample_rows) < 5:
                        sample_rows.append(sanitized_row)
        
        # 生成观测结果
        observation = _format_streaming_observation(row_count, columns, sample_rows, file_path)
        
        return {
            # 重要: 对于 METRIC_QUERY, 不在 State 中存储完整结果
            # 仅存储文件路径, 后续 data_analyzer 从文件读取
            "execution_result": None,  # 显式设为 None, 避免 State 膨胀
            "data_file_path": file_path,
            "execution_observation": observation,
            "execution_error": None,
            "current_node": "sql_executor"
        }
        
    except ImportError:
        return {
            "execution_result": None,
            "data_file_path": None,
            "execution_observation": "ERROR: PyMySQL 未安装",
            "execution_error": "PyMySQL 未安装，无法连接数据库",
            "current_node": "sql_executor"
        }
    except Exception as e:
        # 清理可能产生的不完整文件
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        raise
    finally:
        if cursor:
            cursor.close()
        if owns_connection and connection:
            connection.close()


def _sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """清洗单行数据, 处理特殊类型"""
    result = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            result[k] = float(v)
        elif isinstance(v, (datetime, date)):
            result[k] = v.isoformat()
        elif v is None:
            result[k] = ""
        else:
            result[k] = v
    return result


def _format_streaming_observation(row_count: int, columns: List[str], 
                                   sample_rows: List[Dict], file_path: str) -> str:
    """格式化流式执行的观测结果"""
    if row_count == 0:
        return (
            "Observation: 执行成功。返回了 0 条记录。\n"
            "这意味着 WHERE 条件可能设置得过于严格，或者数据库中不存在匹配的数据。"
        )
    
    sample_str = json.dumps(sample_rows, ensure_ascii=False, indent=2)
    return (
        f"Observation: 执行成功。共 {row_count} 条记录已流式写入临时文件。\n"
        f"文件路径: {file_path}\n"
        f"字段列表: {columns}\n"
        f"前 {len(sample_rows)} 条样本数据:\n{sample_str}\n"
        f"注意: 完整数据已保存到 CSV 文件，后续将由数据分析智能体读取处理。"
    )


def format_observation(result: List[Dict[str, Any]]) -> str:
    """格式化执行结果观测，防止 Token 爆炸"""
    if not result:
        return "Observation: 执行成功。返回了 0 条记录。这意味着 WHERE 条件可能设置得过于严格，或者数据库中不存在匹配该字符串的值（例如全称/简称不匹配）。"
    
    row_count = len(result)
    columns = list(result[0].keys()) if row_count > 0 else []
    
    # 既然结果已经过 _sanitize_results 处理，这里可以直接 dump
    if row_count <= 100:
        res_str = json.dumps(result, ensure_ascii=False)
        return f"Observation: 执行成功。返回了 {row_count} 条记录：\n{res_str}"
    
    # 结果过多（超过100条），进行摘要展示
    sample = result[:5]
    res_str = json.dumps(sample, ensure_ascii=False)
    return (
        f"Observation: 执行成功。返回了大量结果（共 {row_count} 条）。\n"
        f"字段列表: {columns}\n"
        f"前 5 条样本数据: {res_str}\n"
        f"提示：如果你的原始意图是获取宏观统计数据而非海量明细，请确认是否需要增加聚合函数或更严格的筛选条件。"
    )


def is_safe_sql(sql: str) -> bool:
    """检查 SQL 是否安全（只允许 SELECT）"""
    sql_upper = sql.upper().strip()
    
    # 禁止的关键词
    dangerous_keywords = [
        "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE",
        "CREATE", "REPLACE", "GRANT", "REVOKE", "EXEC", "EXECUTE"
    ]
    
    for keyword in dangerous_keywords:
        if keyword in sql_upper:
            return False
    
    return True


def execute_sql(sql: str) -> List[Dict[str, Any]]:
    """执行 SQL 并返回结果 (普通模式)"""
    try:
        import pymysql
        
        connection = pymysql.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database,
            charset=config.database.charset,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
                # 关键：在这里进行清洗，确保流出的数据全是标准 JSON 类型
                return _sanitize_results(list(result))
        finally:
            connection.close()
            
    except ImportError:
        # PyMySQL 未安装，返回模拟结果
        return [{"message": "数据库连接未配置，这是模拟结果", "sql": sql}]
    except Exception as e:
        raise Exception(f"数据库执行错误: {str(e)}")


def _sanitize_results(data: Any) -> Any:
    """递归将结果中的 Decimal 转换为 float，处理 JSON 序列化问题"""
    if isinstance(data, list):
        return [_sanitize_results(item) for item in data]
    elif isinstance(data, dict):
        return {k: _sanitize_results(v) for k, v in data.items()}
    elif isinstance(data, Decimal):
        return float(data)
    elif isinstance(data, (datetime, date)):
        return data.isoformat()
    return data


def execute_with_connection(connection, sql: str) -> List[Dict[str, Any]]:
    """使用已有连接执行 SQL"""
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        # 关键：清洗已有连接返回的结果
        return _sanitize_results(result)
    finally:
        cursor.close()


# 默认节点
sql_executor_node = None
