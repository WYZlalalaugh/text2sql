"""
指标执行器 - 执行SQL并将结果物化到MySQL临时表
Metric Executor - executes SQL and materializes results to MySQL temp tables
"""
from __future__ import annotations

import re
import time
import uuid
from typing import Protocol

from state import AgentState
from config import config


class _DBCursor(Protocol):
    """Protocol for database cursor."""

    rowcount: int
    description: object

    def execute(self, operation: str) -> object | None: ...
    def fetchall(self) -> list[dict[str, object]]: ...
    def close(self) -> None: ...


class _DBConnection(Protocol):
    """Protocol for database connection."""

    def cursor(self, dictionary: bool = False) -> _DBCursor: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...


def create_metric_executor(db_connection_or_manager=None):
    """
    创建指标执行器节点

    Args:
        db_connection_or_manager: 可选的数据库连接对象，或 MetricDBConnectionManager 实例
    """
    
    # 检查是否是连接管理器（有 get_connection 方法）
    is_manager = db_connection_or_manager is not None and hasattr(db_connection_or_manager, 'get_connection')

    def metric_executor_node(state: AgentState) -> dict[str, object]:
        """
        指标执行节点 - 执行SQL并将结果物化到临时表（最后一步直接返回数据）

        输入状态:
        - current_step_id: 当前步骤ID
        - generated_sql: 要执行的SQL
        - metric_plan_nodes: 计划节点列表（用于判断是否为最后一步）

        输出状态:
        - execution_result: 执行结果元数据（中间步骤）或实际数据（最后一步）
        - step_results: 更新的步骤结果
        - materialized_artifacts: 更新的物化产物元数据
        - execution_error: 错误信息（如有）
        - current_node: 当前节点名称
        """
        current_step_id = state.get("current_step_id")
        generated_sql = str(state.get("generated_sql", "") or "")
        metric_plan_nodes = state.get("metric_plan_nodes") or []

        if not current_step_id:
            return {
                "execution_error": "未指定当前步骤ID",
                "current_node": "metric_executor",
            }

        if not generated_sql:
            return {
                "execution_error": "没有SQL可执行",
                "current_node": "metric_executor",
            }

        if not _is_safe_sql(generated_sql):
            return {
                "execution_error": "SQL安全检查未通过（禁止数据修改操作：INSERT/UPDATE/DELETE/DROP等）",
                "current_node": "metric_executor",
            }

        # 判断是否为最后一步
        is_final_step = _is_final_step_in_plan(current_step_id, metric_plan_nodes)
        
        safe_step_id = _sanitize_identifier(str(current_step_id))
        output_table = f"_metric_step_{safe_step_id}_{uuid.uuid4().hex[:8]}"

        try:
            start_time = time.time()
            
            if is_final_step:
                if not _is_select_sql(generated_sql):
                    existing_history = list(state.get("execution_history") or [])
                    history_record = {
                        "step_id": current_step_id,
                        "status": "failed",
                        "sql": generated_sql[:500],
                        "error": "最终步骤必须为 SELECT 语句",
                        "timestamp": int(time.time() * 1000),
                        "is_final_step": True,
                    }
                    updated_history = existing_history + [history_record]
                    return {
                        "execution_error": "最终步骤SQL无效：最后一步必须生成 SELECT 语句",
                        "execution_history": updated_history,
                        "current_node": "metric_executor",
                    }

                # 最后一步：直接执行 SELECT 并返回数据
                result_data = _execute_select_directly(
                    generated_sql, 
                    db_connection_or_manager, 
                    is_manager
                )
                execution_time_ms = int((time.time() - start_time) * 1000)
                
                step_result = {
                    "step_id": current_step_id,
                    "output_table": None,  # 最后一步不创建表
                    "row_count": len(result_data),
                    "columns": list(result_data[0].keys()) if result_data and isinstance(result_data[0], dict) else [],
                    "execution_time_ms": execution_time_ms,
                    "status": "success",
                    "generated_sql": generated_sql,
                    "is_final_step": True,
                }

                existing_step_results = state.get("step_results") or {}
                updated_step_results = dict(existing_step_results)
                updated_step_results[str(current_step_id)] = step_result

                # 添加到执行历史
                existing_history = list(state.get("execution_history") or [])
                history_record = {
                    "step_id": current_step_id,
                    "status": "success",
                    "sql": generated_sql[:500],
                    "timestamp": int(time.time() * 1000),
                    "is_final_step": True,
                }
                updated_history = existing_history + [history_record]

                return {
                    "execution_result": result_data,  # 直接返回数据
                    "execution_error": None,
                    "step_results": updated_step_results,
                    "execution_history": updated_history,
                    "current_node": "metric_executor",
                }
            else:
                # 中间步骤：物化到临时表
                result = _execute_and_materialize(generated_sql, output_table, db_connection_or_manager, is_manager)
                execution_time_ms = int((time.time() - start_time) * 1000)

                # 如果使用连接管理器，注册表名
                if is_manager and db_connection_or_manager is not None and hasattr(db_connection_or_manager, 'register_table'):
                    db_connection_or_manager.register_table(output_table)

                step_result = {
                    "step_id": current_step_id,
                    "output_table": output_table,
                    "row_count": result.get("row_count", 0),
                    "columns": result.get("columns", []),
                    "execution_time_ms": execution_time_ms,
                    "status": "success",
                    "generated_sql": generated_sql,
                    "is_final_step": False,
                }

                existing_step_results = state.get("step_results") or {}
                updated_step_results = dict(existing_step_results)
                updated_step_results[str(current_step_id)] = step_result

                existing_artifacts = state.get("materialized_artifacts") or {}
                updated_artifacts = dict(existing_artifacts)
                updated_artifacts[str(current_step_id)] = {
                    "artifact_type": "mysql_temp_table",
                    "table_name": output_table,
                    "row_count": result.get("row_count", 0),
                    "columns": result.get("columns", []),
                    "created_at_ms": int(time.time() * 1000),
                }

                execution_result = {
                    "step_id": current_step_id,
                    "artifact_type": "mysql_temp_table",
                    "output_table": output_table,
                    "row_count": result.get("row_count", 0),
                    "columns": result.get("columns", []),
                    "execution_time_ms": execution_time_ms,
                }

                # 添加到执行历史
                existing_history = list(state.get("execution_history") or [])
                history_record = {
                    "step_id": current_step_id,
                    "status": "success",
                    "sql": generated_sql,  # 保存完整SQL
                    "timestamp": int(time.time() * 1000),
                }
                updated_history = existing_history + [history_record]

                # 更新物化表 Schema 缓存（关键改进：供后续步骤使用）
                existing_schemas = state.get("materialized_schemas") or {}
                updated_schemas = dict(existing_schemas)
                updated_schemas[str(current_step_id)] = {
                    "table_name": output_table,
                    "columns": result.get("columns", []),
                    "row_count": result.get("row_count", 0),
                    "created_at": int(time.time() * 1000),
                    "source_step": current_step_id,
                }

                return {
                    "execution_result": execution_result,
                    "execution_error": None,
                    "step_results": updated_step_results,
                    "materialized_artifacts": updated_artifacts,
                    "materialized_schemas": updated_schemas,  # 新增：Schema缓存
                    "execution_history": updated_history,
                    "current_node": "metric_executor",
                }

        except Exception as e:
            if not is_final_step:
                _cleanup_temp_table(output_table, db_connection_or_manager, is_manager)
            
            # 添加失败记录到执行历史（保存完整SQL，不截断）
            existing_history = list(state.get("execution_history") or [])
            history_record = {
                "step_id": current_step_id,
                "status": "failed",
                "sql": generated_sql,  # 保存完整SQL，不截断
                "error": str(e),
                "timestamp": int(time.time() * 1000),
            }
            updated_history = existing_history + [history_record]
            
            return {
                "execution_error": f"执行失败: {str(e)}",
                "execution_history": updated_history,
                "current_node": "metric_executor",
            }

    return metric_executor_node


def _execute_and_materialize(
    sql: str,
    output_table: str,
    db_connection_or_manager=None,
    is_manager: bool = False,
) -> dict[str, object]:
    """
    执行SQL并将结果物化到表
    
    Phase 2 优化:
    1. 使用 CREATE TEMPORARY TABLE 代替普通表（减少元数据锁）
    2. 执行前使用 EXPLAIN 预检SQL语法和表存在性

    Args:
        sql: 要执行的SQL
        output_table: 输出表名
        db_connection_or_manager: 数据库连接或连接管理器
        is_manager: 是否是连接管理器

    Returns:
        dict with row_count, output_table, columns
    """
    # 获取实际连接
    if is_manager and db_connection_or_manager is not None:
        conn = db_connection_or_manager.get_connection()
        owns_connection = False
    else:
        conn = db_connection_or_manager
        owns_connection = False
        
        if conn is None:
            import mysql.connector
            
            # 验证数据库配置完整性
            if not hasattr(config, 'database') or config.database is None:
                raise ValueError("数据库配置缺失: config.database 未配置")
            
            db_config = config.database
            required_attrs = ['host', 'port', 'user', 'password', 'database']
            for attr in required_attrs:
                if not hasattr(db_config, attr) or getattr(db_config, attr) is None:
                    raise ValueError(f"数据库配置缺失: config.database.{attr} 未配置")
            
            conn = mysql.connector.connect(
                host=config.database.host,
                port=config.database.port,
                user=config.database.user,
                password=config.database.password,
                database=config.database.database,
                charset=getattr(config.database, 'charset', 'utf8mb4'),
            )
            owns_connection = True

    cursor = conn.cursor(dictionary=True)

    try:
        # Phase 2 优化: 先进行 SQL 安全预检（只检查危险操作，不检查语法）
        safety_error = _safety_sql_check(sql)
        if safety_error:
            raise ValueError(f"SQL安全检查失败: {safety_error}")
        
        # 不再进行 EXPLAIN 预检，直接执行，让数据库返回真实错误
        # 这样SQL生成器可以根据真实错误进行自我纠错
        
        materialize_sql = _build_materialization_sql(sql, output_table)
        _ = cursor.execute(materialize_sql)
        _ = conn.commit()
        row_count = max(getattr(cursor, "rowcount", 0), 0)

        _ = cursor.execute(f"DESCRIBE `{output_table}`")
        columns = cursor.fetchall()

        return {
            "row_count": row_count,
            "output_table": output_table,
            "columns": columns,
        }
    finally:
        _ = cursor.close()
        # 只有在我们自己创建连接时才关闭
        if owns_connection and conn:
            conn.close()


def _safety_sql_check(sql: str) -> str | None:
    """
    安全检查：只检查危险操作，不检查语法正确性
    
    这样可以让SQL生成器根据数据库的真实错误进行自我纠错，
    而不是被预检拦截
    
    Args:
        sql: 要检查的 SQL
        
    Returns:
        错误信息，如果没有危险操作返回 None
    """
    sql_upper = sql.upper().strip()
    
    # 只允许以 SELECT 或 CREATE TABLE 开头的语句
    if not (sql_upper.startswith("SELECT") or 
            sql_upper.startswith("CREATE TABLE") or 
            sql_upper.startswith("CREATE TEMPORARY TABLE")):
        return "只允许 SELECT 和 CREATE TABLE 语句"
    
    # 禁止多语句（分号注入）
    if ";" in sql_upper.rstrip(";"):
        return "禁止多语句执行"
    
    # 检查危险关键字
    dangerous_keywords = [
        r"\bDROP\b",
        r"\bDELETE\b", 
        r"\bUPDATE\b",
        r"\bINSERT\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
    ]
    
    import re
    for pattern in dangerous_keywords:
        if re.search(pattern, sql_upper):
            return f"检测到危险操作: {pattern}"
    
    return None  # 安全检查通过


def _build_materialization_sql(sql: str, output_table: str) -> str:
    """
    将输入SQL转换为固定输出表的物化语句
    
    Phase 2 优化: 使用 CREATE TEMPORARY TABLE 代替普通表
    - 减少元数据锁争用
    - 自动清理（连接关闭时）
    - 对其他会话不可见（隔离性好）
    """
    normalized_sql = sql.strip().rstrip(";")

    create_table_pattern = re.compile(
        r"^CREATE\s+(?:TEMPORARY\s+)?TABLE\s+[`\"]?[A-Za-z0-9_]+[`\"]?",
        re.IGNORECASE,
    )

    if normalized_sql.upper().startswith("CREATE TABLE") or normalized_sql.upper().startswith("CREATE TEMPORARY TABLE"):
        # 替换为 CREATE TEMPORARY TABLE
        return create_table_pattern.sub(f"CREATE TEMPORARY TABLE `{output_table}`", normalized_sql, count=1)

    # 默认使用 CREATE TEMPORARY TABLE AS SELECT
    return f"CREATE TEMPORARY TABLE `{output_table}` AS {normalized_sql}"


def _cleanup_temp_table(
    table_name: str,
    db_connection_or_manager=None,
    is_manager: bool = False,
) -> None:
    """清理物化表（普通表）"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # 获取实际连接
        if is_manager and db_connection_or_manager is not None:
            # 类型断言：连接管理器有 get_connection 方法
            conn = db_connection_or_manager.get_connection()  # type: ignore
        else:
            conn = db_connection_or_manager

        if conn is not None:
            cursor = conn.cursor()
            try:
                _ = cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                _ = conn.commit()
            except Exception as e:
                logger.warning(f"清理临时表 {table_name} 失败: {e}")
            finally:
                _ = cursor.close()
            return

        # 没有提供连接，创建新连接
        import mysql.connector
        
        # 验证数据库配置
        if not hasattr(config, 'database') or config.database is None:
            logger.warning("无法清理临时表 {table_name}: 数据库配置缺失")
            return
        
        conn = mysql.connector.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database,
            charset=getattr(config.database, 'charset', 'utf8mb4'),
        )
        try:
            cursor = conn.cursor()
            try:
                _ = cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                _ = conn.commit()
            except Exception as e:
                logger.warning(f"清理临时表 {table_name} 失败: {e}")
            finally:
                _ = cursor.close()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"清理临时表 {table_name} 时发生异常: {e}")


def _sanitize_identifier(value: str) -> str:
    """将步骤ID转换为可安全拼接到表名的标识符。"""
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_")
    return sanitized or "step"


def _is_final_step_in_plan(current_step_id: str, plan_nodes: list[dict[str, object]]) -> bool:
    """
    判断当前步骤是否为计划中的最后一步。
    
    Args:
        current_step_id: 当前步骤ID
        plan_nodes: 计划节点列表
        
    Returns:
        如果是最后一步返回 True，否则返回 False
    """
    if not plan_nodes:
        return True  # 没有计划节点时，假设是最后一步
    
    # 找到计划列表中最后一个节点
    last_node = plan_nodes[-1]
    last_step_id = last_node.get("step_id")
    
    return str(current_step_id) == str(last_step_id)


def _is_select_sql(sql: str) -> bool:
    return sql.strip().upper().startswith("SELECT")


def _execute_select_directly(
    sql: str,
    db_connection_or_manager=None,
    is_manager: bool = False,
) -> list[dict[str, object]]:
    """
    直接执行 SELECT 语句并返回结果数据。
    
    Phase 2 修复: 执行前使用 EXPLAIN 预检
    
    Args:
        sql: SELECT SQL 语句
        db_connection_or_manager: 数据库连接或连接管理器
        is_manager: 是否是连接管理器
        
    Returns:
        查询结果列表（已清洗）
    """
    from datetime import datetime, date
    from decimal import Decimal
    
    # 获取实际连接
    if is_manager and db_connection_or_manager is not None:
        conn = db_connection_or_manager.get_connection()
        owns_connection = False
    else:
        conn = db_connection_or_manager
        owns_connection = False
        
        if conn is None:
            import mysql.connector
            
            if not hasattr(config, 'database') or config.database is None:
                raise ValueError("数据库配置缺失: config.database 未配置")
            
            db_config = config.database
            required_attrs = ['host', 'port', 'user', 'password', 'database']
            for attr in required_attrs:
                if not hasattr(db_config, attr) or getattr(db_config, attr) is None:
                    raise ValueError(f"数据库配置缺失: config.database.{attr} 未配置")
            
            conn = mysql.connector.connect(
                host=config.database.host,
                port=config.database.port,
                user=config.database.user,
                password=config.database.password,
                database=config.database.database,
                charset=getattr(config.database, 'charset', 'utf8mb4'),
            )
            owns_connection = True

    cursor = conn.cursor(dictionary=True)

    try:
        # Phase 2 修复: 最终步骤也进行安全检查（不检查语法，只检查危险操作）
        safety_error = _safety_sql_check(sql)
        if safety_error:
            raise ValueError(f"SQL安全检查失败: {safety_error}")
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        
        # 清洗数据（处理 Decimal、datetime 等 JSON 不友好类型）
        def sanitize_value(v):
            if isinstance(v, Decimal):
                return float(v)
            if isinstance(v, (datetime, date)):
                return v.isoformat()
            if v is None:
                return None
            return v
        
        sanitized_rows = []
        for row in rows:
            sanitized_row = {k: sanitize_value(v) for k, v in row.items()}
            sanitized_rows.append(sanitized_row)
        
        return sanitized_rows
    finally:
        cursor.close()
        if owns_connection and conn:
            conn.close()


def _is_safe_sql(sql: str) -> bool:
    """
    检查SQL是否安全
    
    允许的操作:
    - SELECT 语句（包括 UNION）
    - CREATE TABLE 语句（用于临时表物化）
    
    禁止的操作:
    - 数据修改: INSERT, UPDATE, DELETE
    - 结构修改: DROP, ALTER, TRUNCATE
    - 权限操作: GRANT, REVOKE
    - 执行操作: EXEC, EXECUTE
    """
    import re
    
    sql_upper = sql.upper().strip()

    # 只允许以 SELECT 或 CREATE TABLE 开头的语句
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("CREATE TABLE") or sql_upper.startswith("CREATE TEMPORARY TABLE")):
        return False

    # 禁止多语句（分号注入）
    if ";" in sql_upper.rstrip(";"):
        return False

    # 使用词边界检查危险关键字，避免误杀字段名（如 updated_at）
    dangerous_keywords = [
        r"\bDROP\b",
        r"\bDELETE\b",
        r"\bUPDATE\b",
        r"\bINSERT\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
    ]

    for pattern in dangerous_keywords:
        if re.search(pattern, sql_upper):
            return False

    return True


__all__ = ["create_metric_executor"]
