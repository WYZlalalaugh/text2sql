"""
SQL 执行节点 - 连接 MySQL 执行 SQL
"""
from typing import Dict, Any, List, Tuple
import json

from state import AgentState
from config import config


def create_sql_executor(db_connection=None):
    """
    创建 SQL 执行节点
    
    Args:
        db_connection: 可选的数据库连接对象
    """
    
    def sql_executor_node(state: AgentState) -> Dict[str, Any]:
        """SQL 执行节点"""
        generated_sql = state.get("generated_sql", "")
        
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
        
        try:
            if db_connection is not None:
                # 使用提供的连接
                result = execute_with_connection(db_connection, generated_sql)
            else:
                # 尝试创建新连接
                result = execute_sql(generated_sql)
            
            return {
                "execution_result": result,
                "execution_error": None,
                "current_node": "sql_executor"
            }
            
        except Exception as e:
            # 结构化错误信息，便于纠错
            error_message = str(e)
            error_type = type(e).__name__
            
            return {
                "execution_result": None,
                "execution_error": f"[{error_type}] {error_message}",
                "current_node": "sql_executor"
            }
    
    return sql_executor_node


def is_safe_sql(sql: str) -> bool:
    """检查 SQL 是否安全（只允许 SELECT）"""
    sql_upper = sql.upper().strip()
    
    # 只允许 SELECT 语句
    if not sql_upper.startswith("SELECT"):
        return False
    
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
    """执行 SQL 并返回结果"""
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
                return list(result)
        finally:
            connection.close()
            
    except ImportError:
        # PyMySQL 未安装，返回模拟结果
        return [{"message": "数据库连接未配置，这是模拟结果", "sql": sql}]
    except Exception as e:
        raise Exception(f"数据库执行错误: {str(e)}")


def execute_with_connection(connection, sql: str) -> List[Dict[str, Any]]:
    """使用已有连接执行 SQL"""
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        cursor.close()


# 默认节点
sql_executor_node = None
