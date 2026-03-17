"""
Python 代码执行器节点 - 在受限环境中安全执行 Python 代码

职责:
1. 接收 LLM 生成的 Python 代码
2. 在受限的沙箱环境中执行
3. 返回执行结果或错误信息

安全措施:
- 仅允许特定的安全库 (pandas, numpy, math, statistics)
- 禁用危险的内置函数
- 禁止文件写入和网络访问
"""
from typing import Dict, Any, Optional, Tuple
import traceback
import os

from state import AgentState


# 允许在分析代码中使用的安全模块
ALLOWED_MODULES = ['pandas', 'numpy', 'math', 'statistics']


def create_python_executor():
    """
    创建 Python 代码执行器节点
    
    此节点独立于 data_analyzer，专门负责代码执行
    """
    
    def python_executor_node(state: AgentState) -> Dict[str, Any]:
        """
        Python 代码执行节点
        
        从 state 读取:
        - analysis_code: 待执行的 Python 代码
        - data_file_path: 数据文件路径
        
        写入 state:
        - analysis_result: 执行结果
        - analysis_error: 执行错误 (如有)
        """
        analysis_code = state.get("analysis_code", "")
        data_file_path = state.get("data_file_path", "")
        
        if not analysis_code:
            return {
                "analysis_result": None,
                "analysis_error": "没有可执行的代码",
                "current_node": "python_executor"
            }
        
        # 检查数据文件
        if data_file_path and not os.path.exists(data_file_path):
            return {
                "analysis_result": None,
                "analysis_error": f"数据文件不存在: {data_file_path}",
                "current_node": "python_executor"
            }
        
        # 执行代码
        result, error = execute_python_code(analysis_code, data_file_path)
        
        return {
            "analysis_result": result,
            "analysis_error": error,
            "current_node": "python_executor"
        }
    
    return python_executor_node


def execute_python_code(code: str, data_file_path: str = None) -> Tuple[Any, Optional[str]]:
    """
    在受限环境中执行 Python 代码
    
    Args:
        code: Python 代码字符串
        data_file_path: 可选的数据文件路径
        
    Returns:
        (result, error) 元组
        - 成功时: (result, None)
        - 失败时: (None, error_message)
    """
    import pandas as pd
    import numpy as np
    import math
    import statistics
    
    # 创建受限的执行环境
    # 仅暴露安全的内置函数
    safe_builtins = {
        # 类型转换
        'int': int,
        'float': float,
        'str': str,
        'bool': bool,
        'list': list,
        'dict': dict,
        'tuple': tuple,
        'set': set,
        
        # 数学和聚合
        'len': len,
        'sum': sum,
        'min': min,
        'max': max,
        'abs': abs,
        'round': round,
        'pow': pow,
        'divmod': divmod,
        
        # 迭代工具
        'range': range,
        'enumerate': enumerate,
        'zip': zip,
        'map': map,
        'filter': filter,
        'sorted': sorted,
        'reversed': reversed,
        
        # 其他安全函数
        'isinstance': isinstance,
        'type': type,
        'any': any,
        'all': all,
        
        # 禁止打印 (防止 side effect)
        'print': lambda *args, **kwargs: None,
    }
    
    # 全局命名空间
    safe_globals = {
        '__builtins__': safe_builtins,
        'pd': pd,
        'np': np,
        'math': math,
        'statistics': statistics,
    }
    
    # 注入 load_data 工具 (Code-Based 模式)
    # 使用智能包装器，捕获 SQL 错误并生成结构化反馈
    _sql_errors = []  # 收集执行过程中的 SQL 错误信息
    
    try:
        from tools.db_client import load_data as _raw_load_data
        from tools.schema_cache import get_schema
        
        def load_data_with_diagnosis(sql: str):
            """包装 load_data，当 SQL 执行失败时提供结构化错误信息"""
            try:
                return _raw_load_data(sql)
            except Exception as e:
                # 构建 SQL 诊断信息
                error_msg = str(e)
                schema = get_schema()
                available_tables = list(schema.keys()) if isinstance(schema, dict) else []
                
                # 记录 SQL 错误详情
                sql_error_detail = (
                    f"SQL 执行失败:\n"
                    f"  错误: {error_msg}\n"
                    f"  失败的 SQL: {sql}\n"
                    f"  可用表名: {', '.join(available_tables)}"
                )
                _sql_errors.append(sql_error_detail)
                
                # 重新抛出，包含诊断信息
                raise RuntimeError(sql_error_detail) from e
        
        safe_globals['load_data'] = load_data_with_diagnosis
    except ImportError:
        # 如果 tools 模块未初始化，提供一个占位函数
        def load_data_placeholder(sql: str):
            raise RuntimeError("load_data 未初始化，请先调用 tools.init_db_client()")
        safe_globals['load_data'] = load_data_placeholder
    
    # 局部命名空间 (代码可访问的变量)
    safe_locals = {
        'data_file_path': data_file_path,  # 保留以兼容旧代码
        'result': None,
    }
    
    try:
        # 执行代码
        exec(code, safe_globals, safe_locals)
        
        # 获取结果
        result = safe_locals.get('result')
        
        # 确保结果可 JSON 序列化
        result = ensure_serializable(result)
        
        return result, None
        
    except SyntaxError as e:
        error_msg = f"代码语法错误: {str(e)}"
        return None, error_msg
        
    except NameError as e:
        error_msg = f"未定义的变量或函数: {str(e)}"
        return None, error_msg
        
    except Exception as e:
        # 如果有 SQL 错误详情，优先使用结构化反馈 (更有助于重试修复)
        if _sql_errors:
            error_msg = "SQL 执行错误:\n" + "\n".join(_sql_errors)
        else:
            error_msg = f"代码执行错误: {type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        return None, error_msg


def ensure_serializable(obj: Any) -> Any:
    """
    确保对象可 JSON 序列化
    
    递归转换 pandas/numpy 对象为 Python 原生类型
    """
    import pandas as pd
    import numpy as np
    
    if obj is None:
        return None
    
    # 基本类型直接返回
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # 列表/元组递归处理
    if isinstance(obj, (list, tuple)):
        return [ensure_serializable(item) for item in obj]
    
    # 字典递归处理
    if isinstance(obj, dict):
        return {str(k): ensure_serializable(v) for k, v in obj.items()}
    
    # pandas DataFrame
    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    
    # pandas Series
    if isinstance(obj, pd.Series):
        return obj.to_list()
    
    # numpy 数组
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    
    # numpy 标量
    if isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    
    # numpy bool
    if isinstance(obj, np.bool_):
        return bool(obj)
    
    # 其他类型转为字符串
    return str(obj)


def clean_code(code: str) -> str:
    """清理代码字符串，移除 markdown 标记"""
    code = code.strip()
    
    # 移除开头的 ```python 或 ```
    if code.startswith("```python"):
        code = code[9:]
    elif code.startswith("```"):
        code = code[3:]
    
    # 移除结尾的 ```
    if code.endswith("```"):
        code = code[:-3]
    
    return code.strip()


# 默认节点
python_executor_node = None
