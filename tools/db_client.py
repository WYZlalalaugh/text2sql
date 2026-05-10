"""
数据库客户端工具 - 提供 load_data 函数

职责:
1. 执行 SQL 查询并返回 DataFrame
2. 强制只读检查 (SELECT only)
3. 封装数据库连接细节

注意: 此模块将被注入到 python_executor 的沙箱环境中
"""
from typing import Optional, Dict, Any
import pandas as pd
import re

# 数据库连接配置 (存储配置而非连接对象，每次调用时创建新连接)
_db_config: Optional[Dict[str, Any]] = None


def init_db_client(db_config: Dict[str, Any] = None, **kwargs):
    """
    初始化数据库客户端
    
    可以传入配置字典，也可以传入关键字参数:
    - init_db_client({"host": "localhost", "port": 3306, ...})
    - init_db_client(host="localhost", port=3306, ...)
    
    Args:
        db_config: 数据库配置字典，包含 host, port, user, password, database, charset
        **kwargs: 如果 db_config 为 None，则使用关键字参数作为配置
    """
    global _db_config
    
    if db_config is not None:
        _db_config = db_config
    elif kwargs:
        _db_config = kwargs
    else:
        # 尝试从 config 模块读取
        try:
            from config import config
            _db_config = {
                "host": config.database.host,
                "port": config.database.port,
                "user": config.database.user,
                "password": config.database.password,
                "database": config.database.database,
                "charset": config.database.charset,
            }
        except Exception as e:
            raise ValueError(f"数据库配置不可用: {e}")
    
    print(f"✓ db_client 初始化完成: {_db_config.get('host')}:{_db_config.get('port')}")


def load_data(sql: str) -> pd.DataFrame:
    """
    执行 SQL 查询并返回 DataFrame
    
    这是提供给 Data Analyzer 生成的代码使用的核心函数。
    它会被注入到 python_executor 的沙箱环境中。
    
    每次调用都会创建新的数据库连接，执行完毕后自动关闭，
    确保连接不会超时，也保证线程安全。
    
    Args:
        sql: SQL 查询语句 (必须是 SELECT)
        
    Returns:
        pd.DataFrame: 查询结果
        
    Raises:
        ValueError: 如果 SQL 包含非 SELECT 语句
        ConnectionError: 如果数据库未初始化
    """
    global _db_config
    
    # 自动初始化配置 (如果尚未初始化)
    if _db_config is None:
        try:
            # 确保项目根目录在 sys.path 中 (解决沙箱环境下的导入路径问题)
            import sys
            import os
            
            # 由于当前文件在 text2sql/tools/db_client.py
            # 向上两级目录就是项目根目录 text2sql/
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(current_dir))
            
            # 如果是 PyInstaller 或其他环境，可能需要不同处理
            # 这里也尝试加入上一级目录
            parent_dir = os.path.dirname(current_dir)
            
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
                
            from config import config
            _db_config = {
                "host": config.database.host,
                "port": config.database.port,
                "user": config.database.user,
                "password": config.database.password,
                "database": config.database.database,
                "charset": config.database.charset,
            }
        except ImportError as e:
            raise ConnectionError(f"自动初始化失败: 无法导入 config 模块。请检查路径配置或先调用 init_tools_db_client()。错误详情: {e} | sys.path: {sys.path}")
        except Exception as e:
            raise ConnectionError(f"自动初始化失败: 配置读取错误: {e}")
    
    # 安全检查: 只允许 SELECT 语句
    if not _is_safe_sql(sql):
        raise ValueError(f"安全检查失败: 只允许 SELECT 语句。收到: {sql[:100]}...")
    
    try:
        from sqlalchemy import create_engine
        import urllib.parse
        
        # 构建数据库连接 URL
        # 格式: mysql+pymysql://user:password@host:port/database?charset=utf8mb4
        escaped_password = urllib.parse.quote_plus(_db_config.get("password", ""))
        db_url = (
            f"mysql+pymysql://{_db_config.get('user', 'root')}:{escaped_password}"
            f"@{_db_config.get('host', 'localhost')}:{_db_config.get('port', 3306)}"
            f"/{_db_config.get('database', '')}"
            f"?charset={_db_config.get('charset', 'utf8mb4')}"
        )
        
        # 创建 SQLAlchemy 引擎
        engine = create_engine(db_url)
        
        try:
            # 使用 pandas 读取 SQL 结果 (使用 read_sql_query 以获得更好的兼容性)
            df = pd.read_sql_query(sql, engine)
            
            # 自动清洗数据: 尝试将 object 类型的列转换为数值
            # 虽然 SQLAlchemy 处理类型更好，但为了保险依然保留此逻辑
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        # 尝试转换为数字，失败则保留原样 (errors='ignore')
                        # 使用 pd.to_numeric 而不是 manual cast
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
            
            return df
        finally:
            # 确保资源释放
            engine.dispose()
            
    except ImportError:
        raise RuntimeError("SQLAlchemy 或 pymysql 未安装，无法连接数据库")
    except Exception as e:
        raise RuntimeError(f"SQL 执行失败: {str(e)}")
            
    except ImportError:
        raise RuntimeError("pymysql 未安装，无法连接数据库")
    except Exception as e:
        raise RuntimeError(f"SQL 执行失败: {str(e)}")


def _is_safe_sql(sql: str) -> bool:
    """
    检查 SQL 是否安全 (只允许 SELECT)
    
    Args:
        sql: SQL 语句
        
    Returns:
        bool: 是否安全
    """
    # 移除注释和多余空白
    cleaned_sql = sql.strip().upper()
    
    # 禁止的关键字列表
    forbidden_keywords = [
        'DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'INSERT', 
        'ALTER', 'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
    ]
    
    # 检查是否以 SELECT 开头
    if not cleaned_sql.startswith('SELECT'):
        # 也允许 WITH ... SELECT (CTE)
        if not cleaned_sql.startswith('WITH'):
            return False
    
    # 检查禁止的关键字
    for keyword in forbidden_keywords:
        # 使用正则确保是独立的关键字，而不是字段名的一部分
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, cleaned_sql):
            return False
    
    return True


def get_db_config():
    """获取当前数据库配置 (用于调试)"""
    return _db_config
