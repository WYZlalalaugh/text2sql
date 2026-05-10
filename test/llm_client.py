# -*- coding: utf-8 -*-
import os
import sys

# 添加项目根目录到 sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from config import config
# # ### SQL SAMPLES
# **示例 1:**
# Question: 查询北京市所有学校的学生人数
# SQL:
# ```sql
# SELECT school_name, student_num FROM schools WHERE province = '北京市' ORDER BY student_num DESC
# ```
# 说明: 简单查询，按学生数降序排列

# **示例 2:**
# Question: 统计各省份的学校数量
# SQL:
# ```sql
# SELECT province, COUNT(*) AS school_count FROM schools GROUP BY province ORDER BY school_count DESC
# ```
# 说明: 聚合查询，使用 COUNT 和 GROUP BY

# **示例 3:**
# Question: 找出学生数量排名前10的学校
# SQL:
# ```sql
# WITH ranked_schools AS (
#     SELECT school_name, province, student_num,
#            DENSE_RANK() OVER (ORDER BY student_num DESC) AS rank_val
#     FROM schools
# )
# SELECT school_name, province, student_num, rank_val
# FROM ranked_schools
# WHERE rank_val <= 10
# ```
# 说明: 排名查询，使用 CTE 和 DENSE_RANK()

# **示例 4:**
# Question: 对比湖北和湖南在基础设施方面的表现（归一化得分）
# SQL:
# ```sql
# WITH province_stats AS (
#     SELECT 
#         s.province, 
#         SUM(q.level1_weight * sa.value) AS total_score 
#     FROM schools s
#     JOIN school_answers sa ON s.id = sa.school_id
#     JOIN questions q ON sa.question_id = q.id
#     WHERE q.level1_name = '基础设施' AND s.province IN ('湖北省', '湖南省')
#     GROUP BY s.province
# )
# SELECT 
#     province, 
#     total_score,
#     (total_score - MIN(total_score) OVER()) / (MAX(total_score) OVER() - MIN(total_score) OVER()) AS normalized_score
# FROM province_stats
# ```
# 说明: 高级对比查询：使用 CTE 和窗口函数 (OVER) 计算所选省份的相对归一化得分。
# ### USER INSTRUCTIONS
# 1. 用户补充说明: 比较两个省份在基础设施一级指标下的总分
# 2. 明确的查询意图: 湖北和湖南两个省份的基础设施一级指标的总分对比 (比较两个省份在基础设施一级指标下的总分)
# 3. 计算类型: chain_weighted_sum
# 4. 筛选条件: province=湖北, 湖南
# 5. 分组字段: province
# 数据库 Schema
SCHEMA = """```json
{
  "questions": {
    "name": "questions",
    "description": "问题定义表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "content",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "问题的描述或唯一标识代码"
      },
      {
        "name": "level3_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "三级指标权重"
      },
      {
        "name": "level1_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标名称"
      },
      {
        "name": "level1_description",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标描述"
      },
      {
        "name": "level1_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标权重"
      },
      {
        "name": "level2_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标名称"
      },
      {
        "name": "level2_description",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标描述"
      },
      {
        "name": "level2_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标权重"
      }
    ],
    "foreign_keys": []
  },
  "schools": {
    "name": "schools",
    "description": "学校基本信息表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "email",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "phone",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "province",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "city",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "district",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "year",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_type",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_area_type",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "class_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "teacher_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "student_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "data_mark",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      }
    ],
    "foreign_keys": []
  },
  "school_answers": {
    "name": "school_answers",
    "description": "学校答案数据表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_id",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": true,
        "description": "外键, 关联到 schools.id"
      },
      {
        "name": "question_id",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": true,
        "description": "外键, 关联到 questions.id"
      },
      {
        "name": "value",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "直接存储的数值答案 (例如数量、比例等)"
      }
    ],
    "foreign_keys": [
      {
        "id": 0,
        "seq": 0,
        "table": "schools",
        "from": "school_id",
        "to": "id",
        "on_update": "CASCADE",
        "on_delete": "CASCADE",
        "match": "NONE"
      },
      {
        "id": 1,
        "seq": 0,
        "table": "questions",
        "from": "question_id",
        "to": "id",
        "on_update": "CASCADE",
        "on_delete": "CASCADE",
        "match": "NONE"
      }
    ]
  }
}
```"""

# 参考信息
EVIDENCE = """### 完整指标体系
### 基础设施
定义: 用于评估学校为师生数字化教学提供的技术支撑情况
包含二级指标:
  - 网络: 学校网络建设情况
  - 终端: 班级教室交互式多媒体终端的使用情况
  - 教室: 教室配备交互式多媒体设备的情况

### REASONING PLAN
1. 用户意图是进行跨省一级指标综合得分对比，具体是针对'基础设施'这一级指标的汇总得分在湖北省和湖南省之间进行对比。
2. 根据指标体系，'基础设施'包含'网络'、'终端'、'教室'三个二级指标，每个二级指标下又包含若干三级指标问题。相关的原始数据存储在schools（学校信息）、school_answers（答案值）、questions（指标定义与权重）三张表中。
3. 需要进行归一化处理。因为筛选范围是特定的两个省份，属于局部范围。为了遵循'全局映射'原则，确保公平对比，必须首先基于全国所有学校的原始答案数据，计算每个三级指标具体测量项（即每个question_id对应的value）的全局最大值和最小值。
4. 规划采用CTE多层架构。CTE1（global_extremes）用于计算每个三级指标（question_id）在全国范围内的最大值和最小值。主查询将筛选后的两省数据与CTE1关联，对每个学校的每个三级指标答案进行归一化得分计算：归一化得分 = (school_answers.value - global_extremes.global_min) / (global_extremes.global_max - global_extremes.global_min)。
5. 应用筛选条件：在schools表中筛选province为'湖北省'或'湖南省'的记录。由于用户查询一级指标汇总，且要求按省对比，最终结果应按schools.province分组。
6. 采用链式加权聚合计算一级指标得分：首先，将每个三级指标的归一化得分乘以其自身的三级权重（questions.level3_weight），得到其对所属二级指标的贡献值。然后，将这些贡献值按二级指标分组求和，并乘以对应的二级权重（questions.level2_weight），得到每个二级指标得分。最后，将所有属于'基础设施'的二级指标得分（'网络'、'终端'、'教室'）求和。
7. 将上一步的得分除上湖北省和湖南省的学校数量（SUM(得分) / COUNT(DISTINCT school_id)），即为湖北省和湖南省在'基础设施'一级指标上的综合得分。
"""

# 用户问题
QUESTION = "湖北和湖南两个省份的基础设施一级指标的总分对比 (比较两个省份在基础设施一级指标下的总分)"

PROMPT = f"""你是一名MySQL专家，现在需要阅读并理解下面的【数据库schema】描述，以及可能用到的【参考信息】，并运用MySQL知识生成sql语句回答【用户问题】。
【用户问题】
{QUESTION}

【数据库schema】
{SCHEMA}

【参考信息】
{EVIDENCE}

【用户问题】
{QUESTION}

```sql"""

PROMPT_v1 = """
### 任务说明
你是一个专业的 Text2SQL 助手，专门为数据库生成准确的 MYSQL 查询语句。

## 你的任务
根据提供的数据库 Schema 和用户的查询意图，生成**准确、高效、安全**的 SQL 查询语句。

## 重要规则

### SQL 规范
1. 只生成 `SELECT` 语句，禁止任何修改操作（INSERT/UPDATE/DELETE/DROP）
2. 使用标准 MySQL 语法
3. 字段名和表名使用反引号 `` ` `` 包裹
4. 字符串值使用单引号 `'` 包裹
5. 中文字段值请使用原始中文，不要转义

### 查询优化
1. 只选择必要的字段，避免 `SELECT *`
2. 合理使用索引字段进行过滤
3. 大数据量时添加 `LIMIT` 限制
4. 使用 `ORDER BY` 确保结果有序

### 聚合查询
1. 使用 `GROUP BY` 时确保所有非聚合字段都在分组中
2. 配合 `COUNT()`, `SUM()`, `AVG()`, `MAX()`, `MIN()` 进行统计
3. 使用 `HAVING` 对聚合结果进行过滤

### 常见模式示例

```sql
-- 统计各省份学校数量
SELECT `省份`, COUNT(*) AS `学校数量`
FROM `schools`
GROUP BY `省份`
ORDER BY `学校数量` DESC;

-- 查询特定条件的数据
SELECT `学校名称`, `学生数`, `教师数`
FROM `schools`
WHERE `省份` = '北京市' AND `年份` = 2023
ORDER BY `学生数` DESC
LIMIT 10;

-- 计算平均值
SELECT `学校类型`, AVG(`教师数`) AS `平均教师数`
FROM `schools`
GROUP BY `学校类型`;
```

## 输出要求
- **只输出 SQL 语句**，不要解释
- 不要添加 markdown 代码块标记
- 确保 SQL 语法正确，可直接执行


---

### 数据库 Schema
```json
{
  "questions": {
    "name": "questions",
    "description": "问题定义表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "content",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "问题的描述或唯一标识代码"
      },
      {
        "name": "level3_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "三级指标权重"
      },
      {
        "name": "level1_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标名称"
      },
      {
        "name": "level1_description",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标描述"
      },
      {
        "name": "level1_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "一级指标权重"
      },
      {
        "name": "level2_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标名称"
      },
      {
        "name": "level2_description",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标描述"
      },
      {
        "name": "level2_weight",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "二级指标权重"
      }
    ],
    "foreign_keys": []
  },
  "schools": {
    "name": "schools",
    "description": "学校基本信息表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "email",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "phone",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "province",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "city",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "district",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_name",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "year",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_type",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_area_type",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "class_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "teacher_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "student_num",
        "type": "TEXT",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "data_mark",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": false,
        "description": null
      }
    ],
    "foreign_keys": []
  },
  "school_answers": {
    "name": "school_answers",
    "description": "学校答案数据表",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "is_primary": true,
        "is_foreign": false,
        "description": null
      },
      {
        "name": "school_id",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": true,
        "description": "外键, 关联到 schools.id"
      },
      {
        "name": "question_id",
        "type": "INTEGER",
        "is_primary": false,
        "is_foreign": true,
        "description": "外键, 关联到 questions.id"
      },
      {
        "name": "value",
        "type": "REAL",
        "is_primary": false,
        "is_foreign": false,
        "description": "直接存储的数值答案 (例如数量、比例等)"
      }
    ],
    "foreign_keys": [
      {
        "id": 0,
        "seq": 0,
        "table": "schools",
        "from": "school_id",
        "to": "id",
        "on_update": "CASCADE",
        "on_delete": "CASCADE",
        "match": "NONE"
      },
      {
        "id": 1,
        "seq": 0,
        "table": "questions",
        "from": "question_id",
        "to": "id",
        "on_update": "CASCADE",
        "on_delete": "CASCADE",
        "match": "NONE"
      }
    ]
  }
}
```

---

### 完整指标体系 ###
```json
### 基础设施 > 网络
定义: 学校网络建设情况

### 基础设施 > 终端
定义: 班级教室交互式多媒体终端的使用情况

### 基础设施 > 教室
定义: 教室配备交互式多媒体设备的情况

```

### SQL SAMPLES ###
**示例 1:**
Question: 查询北京市所有学校的学生人数
SQL:
```sql
SELECT school_name, student_num FROM schools WHERE province = '北京市' ORDER BY student_num DESC
```
说明: 简单查询，按学生数降序排列

**示例 2:**
Question: 统计各省份的学校数量
SQL:
```sql
SELECT province, COUNT(*) AS school_count FROM schools GROUP BY province ORDER BY school_count DESC
```
说明: 聚合查询，使用 COUNT 和 GROUP BY

**示例 3:**
Question: 找出学生数量排名前10的学校
SQL:
```sql
WITH ranked_schools AS (
    SELECT school_name, province, student_num,
           DENSE_RANK() OVER (ORDER BY student_num DESC) AS rank_val
    FROM schools
)
SELECT school_name, province, student_num, rank_val
FROM ranked_schools
WHERE rank_val <= 10
```
说明: 排名查询，使用 CTE 和 DENSE_RANK()


### USER INSTRUCTIONS ###
1. 用户补充说明: 一级指标汇总
2. 明确的查询意图: 查询湖北省和湖南省在'基础设施'指标下的综合得分或整体评估结果的对比 (一级指标汇总)
3. 计算类型: chain_weighted_sum
4. 筛选条件: province=['湖北省', '湖南省']
5. 分组字段: schools.province


### REASONING PLAN ###
1. 用户意图是进行跨省一级指标综合得分对比，具体是针对'基础设施'这一级指标的汇总得分在湖北省和湖南省之间进行对比。
2. 根据指标体系，'基础设施'包含'网络'、'终端'、'教室'三个二级指标，每个二级指标下又包含若干三级指标问题。相关的原始数据存储在schools（学校信息）、school_answers（答案值）、questions（指标定义与权重）三张表中。
3. 需要进行归一化处理。因为筛选范围是特定的两个省份，属于局部范围。为了遵循'全局映射'原则，确保公平对比，必须首先基于全国所有学校的原始答案数据，计算每个三级指标具体测量项（即每个question_id对应的value）的全局最大值和最小值。
4. 规划采用CTE多层架构。CTE1（global_extremes）用于计算每个三级指标（question_id）在全国范围内的最大值和最小值。主查询将筛选后的两省数据与CTE1关联，对每个学校的每个三级指标答案进行归一化得分计算：归一化得分 = (school_answers.value - global_extremes.global_min) / (global_extremes.global_max - global_extremes.global_min)。
5. 应用筛选条件：在schools表中筛选province为'湖北省'或'湖南省'的记录。由于用户查询一级指标汇总，且要求按省对比，最终结果应按schools.province分组。
6. 采用链式加权聚合计算一级指标得分：首先，将每个三级指标的归一化得分乘以其自身的三级权重（questions.level3_weight），得到其对所属二级指标的贡献值。然后，将这些贡献值按二级指标分组求和，并乘以对应的二级权重（questions.level2_weight），得到每个二级指标得分。最后，将所有属于'基础设施'的二级指标得分（'网络'、'终端'、'教室'）求和。
7. 将上一步的得分除上湖北省和湖南省的学校数量（SUM(得分) / COUNT(DISTINCT school_id)），即为湖北省和湖南省在'基础设施'一级指标上的综合得分。

### 用户查询
查询湖北省和湖南省在'基础设施'指标下的综合得分或整体评估结果的对比 (一级指标汇总)

---

### 请生成 SQL 查询语句:
"""

def test_llm_client():
    """测试 LLM 客户端连接"""
    print(f"Connecting to LLM at: {config.llm.api_base}")
    print(f"Model: {config.llm.model_name}")
    
    try:
        llm = ChatOpenAI(
            model="/ai/Qwen-SQL",
            openai_api_base="https://4238f856.r8.cpolar.cn/v1",
            openai_api_key="ms-61da828e-6c70-4551-96a4-3e4a82fa7e3d",
            temperature=0.5
        )
        
        messages = [
            # SystemMessage(content="You are a SQL expert."), # 移除 SystemMessage，有些模型对 SystemMessage 和 Prompt 混合使用敏感
            HumanMessage(content=PROMPT)
        ]
        
        print("\nSending request...")
        response = llm.invoke(messages)
        
        print("\nResponse:")
        print("-" * 50)
        print(response.content)
        print("-" * 50)
        print("\nTest completed successfully!")
        
    except Exception as e:
        print(f"\nError: {e}")

if __name__ == "__main__":
    test_llm_client()
