"""
SQL 规则库

参考 WrenAI 的 TEXT_TO_SQL_RULES，提供通用的 SQL 生成规则。
支持不同数据库类型的特定规则。
"""

from typing import List
from enum import Enum


class DatabaseType(str, Enum):
    """支持的数据库类型"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"
    GENERIC = "generic"


# 通用 SQL 规则（适用于所有数据库）
COMMON_SQL_RULES = """
### 安全规则
- **只使用 SELECT 语句**，禁止 DELETE、UPDATE、INSERT 等修改数据的语句
- 只使用 Schema 中明确定义的表和列
- 不要包含可能改变数据库状态的操作

### 语法规范
- 只在用户明确要求查询所有列时使用 `*`
- 只选择必要的列，避免不必要的 `SELECT *`
- 从多个表选择列时必须使用 `JOIN`
- 优先使用 CTE (Common Table Expression) 而不是子查询
- 不要在生成的 SQL 中包含注释

### 引号规则
- 列名和表名使用双引号包裹
- 字符串字面量使用单引号包裹
- 数值字面量不要加引号
- **示例**：`SELECT "customers"."name" FROM "customers" WHERE "city" = 'Beijing' AND "year" = 2023`

### 大小写处理
- 使用 `lower(table.column) LIKE lower(value)` 进行大小写不敏感的模糊匹配
- 使用 `lower(table.column) = lower(value)` 进行大小写不敏感的精确匹配
- 何时使用 LIKE：用户请求模式或部分匹配，值不够具体
- 何时使用 =：用户请求精确值，没有歧义或模式

### 聚合查询
- 使用 `GROUP BY` 时，确保所有非聚合字段都在分组中
- 聚合函数不允许出现在 WHERE 子句中，应该使用 HAVING 子句过滤聚合结果
- 常用聚合函数：`COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()`

### 排序和限制
- 使用 `ORDER BY` 确保结果有序
- 对于大数据集，使用 `LIMIT` 限制返回行数
- 对于 UNION 查询，只能在最终结果上添加 ORDER BY 和 LIMIT

### 排名问题
- 对于 "top x", "bottom x", "first x", "last x" 等排名问题：
  - 必须使用排名函数 `DENSE_RANK()` 进行排名
  - 然后使用 WHERE 子句过滤结果
  - 必须在最终 SELECT 子句中包含排名列

### 别名使用
- 只在最终 SELECT 子句中使用表/列别名
- 不要在其他子句（WHERE、GROUP BY 等）中使用别名
- 别名中不要使用 '.'，用 '_' 替代
- 参考 Schema 中的 alias 注释来确定合适的别名
"""


# MySQL 特定规则
MYSQL_SPECIFIC_RULES = r"""
### MySQL 特定规则
- 使用反引号 `\`column\`` 包裹标识符
- 日期时间处理使用 `DATE_FORMAT()`, `STR_TO_DATE()` 等函数
- 字符串拼接使用 `CONCAT()` 函数
- 限制结果使用 `LIMIT offset, row_count` 或 `LIMIT row_count OFFSET offset`
"""


# PostgreSQL 特定规则  
POSTGRESQL_SPECIFIC_RULES = """
### PostgreSQL 特定规则
- 使用双引号 `"column"` 包裹标识符
- 日期时间类型转换使用 `CAST(column AS TIMESTAMP)`
- 时间范围比较时必须 CAST 为 `TIMESTAMP WITH TIME ZONE`
- 字符串拼接使用 `||` 运算符或 `CONCAT()` 函数
- 限制结果使用 `LIMIT row_count OFFSET offset`
- 不要使用 `EXTRACT(EPOCH FROM expression)`
- 不要使用 `TO_CHAR` 函数
- 不要使用 INTERVAL 或生成 INTERVAL 类似的表达式
- 不要在 `EXTRACT()` 函数中使用 INTERVAL 数据类型作为参数
- 不要使用 `FILTER(WHERE expression)` 子句
"""


# SQLite 特定规则
SQLITE_SPECIFIC_RULES = """
### SQLite 特定规则
- 使用双引号 `"column"` 包裹标识符
- 日期时间处理使用 `datetime()`, `date()`, `time()` 等函数
- 字符串拼接使用 `||` 运算符
- 限制结果使用 `LIMIT row_count OFFSET offset`
- 部分聚合函数支持有限，避免使用复杂窗口函数
"""


def get_sql_rules(database_type: DatabaseType = DatabaseType.GENERIC) -> str:
    """
    获取完整的 SQL 规则
    
    Args:
        database_type: 数据库类型
        
    Returns:
        完整的 SQL 规则文本
    """
    rules = [COMMON_SQL_RULES]
    
    if database_type == DatabaseType.MYSQL:
        rules.append(MYSQL_SPECIFIC_RULES)
    elif database_type == DatabaseType.POSTGRESQL:
        rules.append(POSTGRESQL_SPECIFIC_RULES)
    elif database_type == DatabaseType.SQLITE:
        rules.append(SQLITE_SPECIFIC_RULES)
    
    return "\n\n".join(rules)


def get_sql_correction_rules() -> str:
    """
    获取 SQL 纠错的额外规则
    
    Returns:
        SQL 纠错规则文本
    """
    return """
### SQL 纠错特定规则
1. 仔细分析错误消息，找到根本原因
2. 检查是否违反了上述任何 SQL 规则
3. 检查是否使用了不存在的表或列
4. 检查语法错误：括号不匹配、关键字拼写错误等
5. 检查逻辑错误：JOIN 条件错误、WHERE 条件不合理等
6. 参考 DATABASE SCHEMA、SQL FUNCTIONS 和 USER INSTRUCTIONS
7. 生成纠正后的 SQL 时，确保遵循所有规则
"""


# 禁止的关键词（用于安全检查）
FORBIDDEN_KEYWORDS = [
    "DROP", "DELETE", "UPDATE", "INSERT", "ALTER", 
    "TRUNCATE", "CREATE", "REPLACE", "GRANT", "REVOKE",
    "EXEC", "EXECUTE", "CALL"
]


def is_safe_sql(sql: str) -> bool:
    """
    检查 SQL 是否安全（只允许 SELECT）
    
    Args:
        sql: SQL 语句
        
    Returns:
        是否安全
    """
    sql_upper = sql.upper().strip()
    
    # 必须是 SELECT 语句
    if not sql_upper.startswith("SELECT"):
        return False
    
    # 不能包含禁止的关键词
    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_upper:
            return False
    
    return True
