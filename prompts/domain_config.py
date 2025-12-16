"""
域配置模块

定义业务域的抽象配置接口，支持不同领域的定制化提示词。
参考 WrenAI 的设计模式，通过配置化方式支持多业务场景。
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum

from .sql_rules import DatabaseType
from .sql_samples import SQLSampleLibrary


class IntentType(str, Enum):
    """意图类型（可扩展）"""
    SIMPLE_QUERY = "simple_query"
    METRIC_QUERY = "metric_query"
    METRIC_DEFINITION = "metric_definition"
    CHITCHAT = "chitchat"


@dataclass
class TableSchema:
    """表结构描述"""
    name: str                    # 表名
    description: str             # 表描述
    columns: List[Dict[str, str]]  # 列信息 [{"name": "列名", "type": "类型", "description": "描述"}]
    alias: Optional[str] = None  # 表别名


@dataclass
class DomainConfig:
    """业务域配置基类"""
    
    # 基本信息
    name: str                             # 域名称
    description: str                      # 域描述
    database_type: DatabaseType = DatabaseType.MYSQL  # 数据库类型
    
    # 数据结构
    tables: List[TableSchema] = field(default_factory=list)  # 表结构
    
    # 业务规则
    business_rules: List[str] = field(default_factory=list)  # 业务规则
    metric_definitions: Dict[str, str] = field(default_factory=dict)  # 指标定义
    
    # 查询示例
    sql_samples: Optional[SQLSampleLibrary] = None  # SQL 示例库
    
    # SQL 函数说明
    sql_functions: List[str] = field(default_factory=list)  # 数据库函数说明
    
    # 意图分类
    supported_intents: List[IntentType] = field(default_factory=lambda: [
        IntentType.SIMPLE_QUERY,
        IntentType.METRIC_QUERY,
        IntentType.METRIC_DEFINITION,
        IntentType.CHITCHAT,
    ])
    
    # 歧义检测规则
    ambiguity_rules: List[str] = field(default_factory=list)
    
    def get_schema_description(self) -> str:
        """获取完整的 Schema 描述"""
        lines = []
        for table in self.tables:
            lines.append(f"Table: {table.name}")
            if table.description:
                lines.append(f"Description: {table.description}")
            if table.alias:
                lines.append(f"Alias: {table.alias}")
            lines.append("Columns:")
            for col in table.columns:
                col_line = f"  - {col['name']} ({col.get('type', 'UNKNOWN')})"
                if col.get('description'):
                    col_line += f": {col['description']}"
                if col.get('alias'):
                    col_line += f" [alias: {col['alias']}]"
                lines.append(col_line)
            lines.append("")  # 空行分隔
        return "\n".join(lines)
    
    def get_metric_definitions_text(self) -> str:
        """获取指标定义文本"""
        if not self.metric_definitions:
            return ""
        lines = ["### 指标体系定义 ###"]
        for metric_name, definition in self.metric_definitions.items():
            lines.append(f"**{metric_name}**: {definition}")
        return "\n".join(lines)
    
    def get_business_rules_text(self) -> str:
        """获取业务规则文本"""
        if not self.business_rules:
            return ""
        lines = ["### 业务规则 ###"]
        for i, rule in enumerate(self.business_rules, 1):
            lines.append(f"{i}. {rule}")
        return "\n".join(lines)


# 教育指标域配置（默认实现）
class EducationDomain(DomainConfig):
    """教育指标体系域配置"""
    
    def __init__(self):
        from .sql_samples import get_education_samples
        
        super().__init__(
            name="教育指标体系",
            description="用于评估学校教育数字化水平的数据库",
            database_type=DatabaseType.MYSQL,
            metric_definitions={
                "基础设施": "评估学校为师生数字化教学提供的技术支撑情况（网络、终端、教室）",
                "数字资源": "评估数字教育资源的建设和应用情况（规模、质量、应用）",
                "教育教学": "评估教学、评价等要素的数字化程度（教学方式、教学评价）",
                "数字素养": "评估学生和教师的数字技术思维和应用能力",
                "教育治理": "学校利用数字化改革赋能校园治理现代化（学校治理、政务服务、网络安全）",
                "保障机制": "评估教育数字化保障能力（组织保障、人力保障、财力保障）",
            },
            business_rules=[
                "优先使用标准化的指标表字段进行查询",
                "对于涉及多年数据对比的查询，需明确指定年份范围",
                "涉及地区筛选时，支持省-市-区县三级筛选",
                "指标得分通常为 0-100 分的数值",
            ],
            sql_samples=get_education_samples(),
            ambiguity_rules=[
                "用户只提一级指标时，需澄清是查看所有二级指标明细还是计算综合得分",
                "缺少年份时，需澄清查询的时间范围",
                "缺少地区范围时，需澄清是全国、特定省市还是特定学校",
                "查询目标不明时，需澄清是单个学校数据、学校对比还是区域汇总",
            ]
        )


# 全局域配置注册表
_domain_registry: Dict[str, DomainConfig] = {}


def register_domain(domain: DomainConfig):
    """注册一个域配置"""
    _domain_registry[domain.name] = domain


def get_domain(name: str) -> Optional[DomainConfig]:
    """获取域配置"""
    return _domain_registry.get(name)


def list_domains() -> List[str]:
    """列出所有已注册的域"""
    return list(_domain_registry.keys())


# 注册默认域
register_domain(EducationDomain())
