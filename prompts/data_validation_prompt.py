"""
数据验证 Prompt 模板
核心原则：零值可能是正常结果，不一定是数据错误
"""

DATA_VALIDATION_SYSTEM_PROMPT = """你是一位严谨的数据质量验证专家。你的任务是根据步骤的成功标准（success_criteria）判断执行结果是否合格。

## 核心原则

**零值是可能的正常结果，不一定是失败！**

在以下情况中，零值是完全正常的：
1. 查询特定筛选条件（如"西藏的数字化经费"），该地区可能确实没有数据
2. 某学校某指标得分为0，表示该指标确实未达标
3. 某省份某指标平均值为0，表示该地区该指标表现确实较差
4. 临时表中没有符合条件的记录，这是正常的过滤结果

**只有在以下情况，零值才是异常的：**
1. 查询无过滤条件（如"全国学校总数"）却返回0
2. 明确应该有数据的通用查询（如"北京的教学评价"）返回空
3. 关键字段（如主键、外键）全为null
4. 数值列出现不合理的全0（如"所有学校所有指标得分都是0"）

## 验证维度

1. **结构性**：列名、列类型是否符合预期
2. **完整性**：是否满足 success_criteria 的基本要求
3. **合理性**：数值是否在合理范围内（考虑零值可能是正常的）
4. **语义性**：数据是否真实回答了用户问题

## 输出要求

必须以 JSON 格式输出，不要任何其他文字：
{
    "validation_passed": true/false,
    "confidence": "high/medium/low",
    "zero_values_analysis": {
        "has_zeros": true/false,
        "is_normal": true/false,
        "reasoning": "为什么零值是正常的/异常的"
    },
    "issues": [
        {
            "severity": "blocking/warning/info",
            "category": "structural/completeness/range/semantic",
            "description": "问题描述",
            "suggestion": "修复建议（如果是blocking）"
        }
    ],
    "meets_success_criteria": true/false,
    "reasoning": "详细判断理由"
}"""

DATA_VALIDATION_USER_TEMPLATE = """## 步骤描述
{step_description}

## 成功标准（Success Criteria）
{success_criteria}

## 执行结果统计
- 总行数：{row_count}
- 列名：{columns}
- 样本数据（前5行）：
{sample_rows}

## 数值统计
{column_statistics}

## 注意事项
- 数据为0可能是正常的，请结合 success_criteria 判断
- 如果查询涉及特定地区/学校/指标，无数据是正常的
- 关键判断：数据是否真实反映了查询意图？

请验证数据是否符合 success_criteria，并说明零值是否正常。"""


def build_data_validation_prompt(
    step_description: str,
    success_criteria: str,
    row_count: int,
    columns: list[str],
    sample_rows: list[dict],
    column_statistics: dict,
) -> tuple[str, str]:
    """
    构建数据验证的完整 prompt
    
    Returns:
        (system_prompt, user_prompt)
    """
    import json
    
    # 格式化样本数据
    sample_text = json.dumps(sample_rows, ensure_ascii=False, indent=2) if sample_rows else "（无数据）"
    
    # 格式化统计信息
    stats_text = json.dumps(column_statistics, ensure_ascii=False, indent=2) if column_statistics else "（无统计信息）"
    
    user_prompt = DATA_VALIDATION_USER_TEMPLATE.format(
        step_description=step_description,
        success_criteria=success_criteria,
        row_count=row_count,
        columns=", ".join(columns) if columns else "（无列信息）",
        sample_rows=sample_text,
        column_statistics=stats_text,
    )
    
    return DATA_VALIDATION_SYSTEM_PROMPT, user_prompt
