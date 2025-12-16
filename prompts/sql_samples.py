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
        "sql": 'SELECT "学校名称", "学生数" FROM "schools" WHERE "省份" = \'北京市\' ORDER BY "学生数" DESC',
        "description": "简单查询，按学生数降序排列"
    },
    {
        "question": "统计各省份的学校数量",
        "sql": 'SELECT "省份", COUNT(*) AS "学校数量" FROM "schools" GROUP BY "省份" ORDER BY "学校数量" DESC',
        "description": "聚合查询，使用 COUNT 和 GROUP BY"
    },
    {
        "question": "找出学生数量排名前10的学校",
        "sql": '''WITH ranked_schools AS (
    SELECT "学校名称", "省份", "学生数",
           DENSE_RANK() OVER (ORDER BY "学生数" DESC) AS rank
    FROM "schools"
)
SELECT "学校名称", "省份", "学生数", rank
FROM ranked_schools
WHERE rank <= 10''',
        "description": "排名查询，使用 CTE 和 DENSE_RANK()"
    },
    {
        "question": "对比北京和上海两地的平均班级数",
        "sql": '''SELECT "省份", AVG("班级数") AS "平均班级数"
FROM "schools"
WHERE "省份" IN ('北京市', '上海市')
GROUP BY "省份"''',
        "description": "对比查询，使用 IN 和 AVG"
    }
]


def get_education_samples() -> SQLSampleLibrary:
    """获取教育指标领域的示例库"""
    library = SQLSampleLibrary()
    for sample in EDUCATION_SQL_SAMPLES:
        library.add_sample(**sample)
    return library
