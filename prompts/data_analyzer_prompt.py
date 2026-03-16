"""
数据分析智能体提示词模板 (Code-Based 模式)

指导 LLM 生成包含 SQL 查询和数据分析的完整 Python 代码。
使用 load_data(sql) 函数直接从数据库获取数据。
"""

# 数据分析代码生成系统提示词
DATA_ANALYZER_SYSTEM_PROMPT = """你是一个专业的数据分析工程师。你的任务是根据查询计划和数据库结构，编写完整的 Python 代码来获取数据并计算指标。

## 核心规则

### 可用工具 (已预先导入，禁止使用 import 语句!)
以下库和函数已经预先导入到执行环境中，**请勿使用 import 语句**：
- `load_data(sql: str) -> pd.DataFrame`: 执行 SQL 查询并返回 DataFrame
- `pd`: pandas 库 (已导入)
- `np`: numpy 库 (已导入)
- `math`: math 模块 (已导入)
- `statistics`: statistics 模块 (已导入)

**重要**: 不要写 `import pandas as pd` 等语句，直接使用 `pd`、`np` 即可！

### 代码规范
1. 使用 `load_data(sql)` 获取数据，SQL 必须是 SELECT 语句
2. 最终结果必须赋值给变量 `result`
3. `result` 必须是可 JSON 序列化的类型 (dict, list, float, int, str)
4. 如果结果是 DataFrame，转换为 `result = df.to_dict(orient='records')`
5. 代码必须是完整可执行的，不要使用占位符
6. **禁止使用 import 语句**

### 防御性编程 (重要!)
1. 执行计算前**必须**检查 DataFrame 是否为空: `if df.empty: result = "无数据"`
2. 处理可能的除零错误
3. 处理可能的空值 (NaN)

### SQL 编写规范
1. 只使用 SELECT 语句，禁止 UPDATE/DELETE/DROP
2. 字段名和表名使用 Schema 中的准确名称
3. 合理使用 WHERE 条件过滤数据
4. 如需多次查询，可多次调用 load_data()

## 输出格式
只返回 Python 代码块，不要解释，不要使用 import:
```python
# 直接使用 pd, np, load_data 等，无需 import
```
"""

# 数据分析代码生成提示词模板
DATA_ANALYZER_PROMPT_TEMPLATE = """{system_prompt}

---

### 用户查询 ###
{user_query}

---

### 数据库 Schema ###
{schema_context}

---

### 查询计划 (请严格按此逻辑实现) ###
{query_plan_context}

---

{verification_context}

请生成完整的 Python 分析代码:
```python
"""


def build_data_analyzer_prompt(
    user_query: str,
    schema_context: str = "",
    query_plan: dict = None,
    verification_feedback: str = None,
    selected_metrics: list = None,
    metrics_definitions: dict = None
) -> str:
    """
    构建数据分析代码生成 Prompt (Code-Based 模式)
    
    Args:
        user_query: 用户原始查询
        schema_context: 数据库 Schema 字符串
        query_plan: 查询规划 (包含 reasoning_steps, filters 等)
        verification_feedback: 验证器反馈 (重试时提供)
        selected_metrics: 选中的指标列表 (可选)
        metrics_definitions: 指标定义字典 (可选)
        
    Returns:
        完整的 Prompt 字符串
    """
    # 格式化查询计划上下文
    query_plan_context = ""
    if query_plan:
        reasoning_steps = query_plan.get("reasoning_steps", [])
        if reasoning_steps:
            query_plan_context = "推理步骤:\n"
            for i, step in enumerate(reasoning_steps, 1):
                query_plan_context += f"{i}. {step}\n"
        
        # 添加筛选条件
        filters = query_plan.get("filters", {})
        if filters:
            query_plan_context += "\n筛选条件:\n"
            for key, value in filters.items():
                if value:
                    query_plan_context += f"- {key}: {value}\n"
        
        # 添加计算类型
        calc_type = query_plan.get("calculation_type", "")
        if calc_type:
            query_plan_context += f"\n计算类型: {calc_type}\n"
        
        # 添加目标字段
        target_fields = query_plan.get("target_fields", [])
        if target_fields:
            query_plan_context += f"\n目标字段: {', '.join(target_fields)}\n"
        
        # 添加涉及的表
        involved_tables = query_plan.get("involved_tables", [])
        if involved_tables:
            query_plan_context += f"涉及表: {', '.join(involved_tables)}\n"
    
    if not query_plan_context:
        query_plan_context = "(无查询计划，请根据用户查询和 Schema 自行设计)"
    
    # 格式化验证反馈上下文 (重试时使用)
    verification_context = ""
    if verification_feedback:
        verification_context = f"""### 上次执行反馈 (请修复以下问题) ###
{verification_feedback}

---
"""
    
    # 添加指标定义上下文 (如果提供)
    if selected_metrics and metrics_definitions:
        metrics_context = "\n### 相关指标定义 ###\n"
        for metric_path in selected_metrics:
            parts = [p.strip() for p in metric_path.split(">")]
            level1 = parts[0] if len(parts) >= 1 else ""
            level2 = parts[1] if len(parts) >= 2 else None
            
            if level1 in metrics_definitions:
                level1_data = metrics_definitions[level1]
                if level2:
                    level2_dict = level1_data.get("二级指标", {})
                    if level2 in level2_dict:
                        metrics_context += f"- {level1} > {level2}: {level2_dict[level2].get('二级指标解释', '')}\n"
                else:
                    metrics_context += f"- {level1}: {level1_data.get('一级指标解释', '')}\n"
        
        query_plan_context = metrics_context + "\n" + query_plan_context
    
    return DATA_ANALYZER_PROMPT_TEMPLATE.format(
        system_prompt=DATA_ANALYZER_SYSTEM_PROMPT,
        user_query=user_query,
        schema_context=schema_context if schema_context else "(Schema 未提供)",
        query_plan_context=query_plan_context,
        verification_context=verification_context
    )


# ==================== 向后兼容 ====================
# 保留旧函数签名以兼容可能的调用
def get_csv_sample(file_path: str, max_rows: int = 5) -> str:
    """[已废弃] 保留以兼容旧代码"""
    return "(CSV 模式已废弃，请使用 Code-Based 模式)"
