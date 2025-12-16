"""
Text2SQL 提示词模块

集中管理所有 Agent 使用的提示词，便于维护和版本控制。
"""

# 原有提示词
from .intent_classifier_prompt import INTENT_CLASSIFIER_PROMPT
from .ambiguity_checker_prompt import AMBIGUITY_CHECKER_PROMPT
from .context_assembler_prompt import (
    SQL_GENERATOR_INSTRUCTION,
    SQL_GENERATOR_PROMPT_TEMPLATE
)
from .response_prompt import (
    CHITCHAT_PROMPT,
    QUERY_RESULT_PROMPT,
    HELP_RESPONSE,
    GREETING_RESPONSE
)

# 新增模块
from .sql_rules import (
    DatabaseType,
    get_sql_rules,
    get_sql_correction_rules,
    is_safe_sql,
    COMMON_SQL_RULES
)
from .sql_samples import SQLSampleLibrary, SQLSample, get_education_samples
from .sql_correction_prompt import (
    get_sql_correction_system_prompt,
    build_sql_correction_prompt
)
from .domain_config import (
    DomainConfig,
    EducationDomain,
    TableSchema,
    IntentType,
    register_domain,
    get_domain,
    list_domains
)
from .prompt_builder import PromptBuilder

__all__ = [
    # 原有提示词
    "INTENT_CLASSIFIER_PROMPT",
    "AMBIGUITY_CHECKER_PROMPT",
    "SQL_GENERATOR_INSTRUCTION",
    "SQL_GENERATOR_PROMPT_TEMPLATE",
    "CHITCHAT_PROMPT",
    "QUERY_RESULT_PROMPT",
    "HELP_RESPONSE",
    "GREETING_RESPONSE",
    
    # SQL 规则
    "DatabaseType",
    "get_sql_rules",
    "get_sql_correction_rules",
    "is_safe_sql",
    "COMMON_SQL_RULES",
    
    # SQL 示例
    "SQLSampleLibrary",
    "SQLSample",
    "get_education_samples",
    
    # SQL 纠错
    "get_sql_correction_system_prompt",
    "build_sql_correction_prompt",
    
    # 域配置
    "DomainConfig",
    "EducationDomain",
    "TableSchema",
    "IntentType",
    "register_domain",
    "get_domain",
    "list_domains",
    
    # 提示词构建器
    "PromptBuilder",
]
