"""
提示词构建器 - 支持全量指标上下文
"""

from typing import List, Optional, Dict, Any
import json

from .domain_config import DomainConfig
from .sql_rules import get_sql_rules
from .sql_samples import SQLSampleLibrary
from .context_assembler_prompt import SQL_GENERATOR_INSTRUCTION


class PromptBuilder:
    """动态提示词构建器"""
    
    def __init__(self, domain: DomainConfig):
        self.domain = domain
    
    def _format_metric_context(self, matched_metrics: Optional[List], full_context: Optional[str] = None) -> str:
        """格式化指标上下文"""
        if full_context:
            return f"### 完整指标体系 ###\n```json\n{full_context}\n```"
            
        if matched_metrics:
            metric_context = "### 相关指标信息 ###\n"
            for m in matched_metrics[:5]:
                # 统一处理对象或字典
                if hasattr(m, 'level1_name'):
                    name = m.level1_name
                    desc = m.level1_description
                    if hasattr(m, 'level2_name') and m.level2_name:
                         name += f" > {m.level2_name}"
                         desc = m.level2_description
                elif isinstance(m, dict):
                    name = m.get('level1_name', '')
                    desc = m.get('level1_description', '')
                    if m.get('level2_name'):
                        name += f" > {m.get('level2_name')}"
                        desc = m.get('level2_description')
                else:
                    continue
                metric_context += f"- **{name}**: {desc}\n"
            return metric_context
            
        return "无特定指标信息"

    def build_sql_generation_prompt(
        self,
        query: str,
        schema: dict,
        matched_metrics: list = None,
        sql_samples: SQLSampleLibrary = None,
        instructions: List[str] = None,
        reasoning_plan: str = None,
        full_metrics_context: str = None, 
    ) -> str:
        """
        构建 SQL 生成提示词
        """
        # Schema
        schema_str = json.dumps(schema, ensure_ascii=False, indent=2)
        
        # 指标上下文 (优先使用 full_metrics_context)
        metric_context = self._format_metric_context(matched_metrics, full_metrics_context)
        
        # SQL 示例
        samples_text = ""
        if sql_samples:
            samples_text = "### SQL SAMPLES ###\n" + sql_samples.to_prompt_format(limit=3)
        
        # 用户指令
        instructions_text = ""
        if instructions:
            instructions_text = "### USER INSTRUCTIONS ###\n"
            for i, inst in enumerate(instructions, 1):
                instructions_text += f"{i}. {inst}\n"
        
        # 推理计划
        reasoning_text = ""
        if reasoning_plan:
            reasoning_text = f"### REASONING PLAN ###\n{reasoning_plan}\n"
        
        # 组装完整提示词
        prompt = f"""### 任务说明
{SQL_GENERATOR_INSTRUCTION}

---

### 数据库 Schema
```json
{schema_str}
```

---

{metric_context}

{samples_text}

{instructions_text}

{reasoning_text}

### 用户查询
{query}

---

### 请生成 SQL 查询语句:
"""
        return prompt
    
    def build_intent_classification_prompt(
        self,
        query: str,
        matched_metrics: list = None,
        full_metrics_context: str = None,
    ) -> str:
        """
        构建意图分类提示词
        """
        # 指标信息
        if full_metrics_context:
            metrics_text = f"请参考以下完整指标体系：\n{full_metrics_context[:1000]}..." # 截断以防过长
        elif matched_metrics:
            metrics_text = "\n".join([
                f"- {m.level1_name}" + (f" > {m.level2_name}" if hasattr(m, 'level2_name') and m.level2_name else "") 
                for m in matched_metrics[:5]
            ])
        else:
            metrics_text = "无特定指标匹配"
        
        # 获取域上下文
        domain_description = self.domain.description
        metric_definitions = self.domain.get_metric_definitions_text()
        
        from .intent_classifier_prompt import INTENT_CLASSIFIER_PROMPT
        
        return INTENT_CLASSIFIER_PROMPT.format(
            domain_description=domain_description,
            metric_definitions=metric_definitions,
            matched_metrics=metrics_text,
            user_query=query
        )
    
    def build_ambiguity_check_prompt(
        self,
        query: str,
        matched_metrics: list = None,
        conversation_history: str = "",
        full_metrics_context: str = None,
    ) -> str:
        """
        构建歧义检测提示词
        """
        # 指标信息
        if full_metrics_context:
            metrics_text = f"（完整指标体系已包含，此处省略详细列表）"
        elif matched_metrics:
            metrics_text = "\n".join([
               f"- {m.level1_name}" + (f" > {m.level2_name}" if hasattr(m, 'level2_name') and m.level2_name else "") 
               for m in matched_metrics[:5]
            ])
        else:
            metrics_text = "无特定指标匹配"

        domain_description = self.domain.description
        database_schema_summary = self.domain.get_schema_description()
        metric_structure = full_metrics_context if full_metrics_context else self.domain.get_metric_definitions_text()
        
        from .ambiguity_checker_prompt import DEFAULT_FILTER_CONDITIONS_GUIDANCE
        from .ambiguity_checker_prompt import AMBIGUITY_CHECKER_PROMPT
        
        return AMBIGUITY_CHECKER_PROMPT.format(
            domain_description=domain_description,
            database_schema_summary=database_schema_summary,
            metric_structure=metric_structure,
            matched_metrics=metrics_text,
            user_query=query,
            conversation_history=conversation_history or "无",
            filter_conditions_guidance=DEFAULT_FILTER_CONDITIONS_GUIDANCE
        )
