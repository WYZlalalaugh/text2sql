"""
SQL 示例查询库

用于提供 Question + SQL 配对示例，帮助模型学习特定数据库的查询模式。
"""

from typing import List, Dict, Any
from dataclasses import dataclass
import json
import os


@dataclass
class SQLSample:
    """SQL 示例"""
    question: str     # 用户问题
    sql: str         # 对应的 SQL
    description: str = ""  # 可选的说明


class SQLSampleLibrary:
    """SQL 示例库"""
    
    def __init__(self):
        self.samples: List[SQLSample] = []
    
    def add_sample(self, question: str, sql: str, description: str = ""):
        """添加一个示例"""
        self.samples.append(SQLSample(
            question=question,
            sql=sql,
            description=description
        ))
    
    def get_samples(self, limit: int = None) -> List[SQLSample]:
        """获取示例列表"""
        if limit:
            return self.samples[:limit]
        return self.samples
    
    def load_from_json(self, file_path: str):
        """从 JSON 文件加载示例"""
        if not os.path.exists(file_path):
            return
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for item in data:
            self.add_sample(
                question=item.get("question", ""),
                sql=item.get("sql", ""),
                description=item.get("description", "")
            )
    
    def to_prompt_format(self, limit: int = 5) -> str:
        """
        转换为提示词格式
        
        Args:
            limit: 最多返回多少个示例
            
        Returns:
            格式化的示例文本
        """
        samples = self.get_samples(limit)
        if not samples:
            return ""
        
        result = []
        for i, sample in enumerate(samples, 1):
            result.append(f"**示例 {i}:**")
            result.append(f"Question: {sample.question}")
            result.append(f"SQL:")
            result.append(f"```sql\n{sample.sql}\n```")
            if sample.description:
                result.append(f"说明: {sample.description}")
            result.append("")  # 空行分隔
        
        return "\n".join(result)


# 教育指标领域的默认示例
EDUCATION_SQL_SAMPLES = [
    {
        "question": "查询北京市所有学校的学生人数",
        "sql": "SELECT school_name, student_num FROM schools WHERE province = '北京市' ORDER BY student_num DESC",
        "description": "简单查询，按学生数降序排列"
    },
    {
        "question": "统计各省份的学校数量",
        "sql": "SELECT province, COUNT(*) AS school_count FROM schools GROUP BY province ORDER BY school_count DESC",
        "description": "聚合查询，使用 COUNT 和 GROUP BY"
    },
    {
        "question": "找出学生数量排名前10的学校",
        "sql": """WITH ranked_schools AS (
    SELECT school_name, province, student_num,
           DENSE_RANK() OVER (ORDER BY student_num DESC) AS rank_val
    FROM schools
)
SELECT school_name, province, student_num, rank_val
FROM ranked_schools
WHERE rank_val <= 10""",
        "description": "排名查询，使用 CTE 和 DENSE_RANK()"
    },
    {
         "question": "对比湖北和湖南在基础设施方面的表现",
         "sql": """WITH 
all_province_scores AS (
    SELECT 
        T3.province, 
        SUM(T1.level1_weight * T2.value) AS total_score 
    FROM questions AS T1 
    INNER JOIN school_answers AS T2 ON T1.id = T2.question_id 
    INNER JOIN schools AS T3 ON T2.school_id = T3.id 
    WHERE T1.level1_name = '基础设施'
    GROUP BY T3.province
),
global_stats AS (
    SELECT 
        MAX(total_score) AS max_score, 
        MIN(total_score) AS min_score 
    FROM all_province_scores
)
SELECT 
    p.province, 
    p.total_score,
    CASE 
        WHEN g.max_score = g.min_score THEN 0 
        ELSE (p.total_score - g.min_score) / (g.max_score - g.min_score) 
    END AS normalized_score
FROM all_province_scores AS p 
JOIN global_stats AS g ON 1=1  -- 笛卡尔积连接统计数据
WHERE p.province IN ('湖北省', '湖南省');""",
         "description": "高级对比查询：使用 CTE 计算各省加权总分，再计算全局最大最小值进行 Min-Max 归一化，最后筛选特定省份。"
    }
]


def get_education_samples() -> SQLSampleLibrary:
    """获取教育指标领域的示例库"""
    library = SQLSampleLibrary()
    for sample in EDUCATION_SQL_SAMPLES:
        library.add_sample(**sample)
    return library
