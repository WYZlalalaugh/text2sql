
import json
import logging
import datetime
from typing import Dict, Any, List

import pandas as pd
from langchain_core.messages import HumanMessage, SystemMessage

from config import config
from prompts.chart_prompt import CHART_GENERATION_PROMPT, CHART_INSTRUCTIONS

# 配置日志
logger = logging.getLogger(__name__)

def create_chart_generator():
    """创建一个图表生成器代理"""
    
    from langchain_openai import ChatOpenAI
    
    # 使用配置中的 LLM
    llm = ChatOpenAI(
        model=config.llm.model_name,
        openai_api_base=config.llm.api_base,
        openai_api_key=config.llm.api_key,
        temperature=0.0  # 图表生成需要精确
    )
    
    def chart_generator_node(state: Dict[str, Any]) -> Dict[str, Any]:
        """图表生成逻辑"""
        
        user_query = state.get("user_query", "")
        generated_sql = state.get("generated_sql", "")
        execution_result = state.get("execution_result", [])
        
        # 1. 如果没有数据，直接返回
        if not execution_result:
            return {"chart_spec": None}
            
        # 2. 预处理数据 (采样)
        try:
            # 转换为 DataFrame
            df = pd.DataFrame(execution_result)
            
            # 获取列名
            columns = df.columns.tolist()
            
            # 采样数据 (取前 5 行作为样本)
            sample_data_count = 5
            if len(df) > sample_data_count:
                sample_data = df.head(sample_data_count).to_dict(orient="records")
            else:
                sample_data = df.to_dict(orient="records")
                
            # 采样列值 (每列取前 5 个唯一值)
            sample_column_values = {}
            for col in df.columns:
                unique_vals = df[col].unique().tolist()[:5]
                # 转换 Decimal 或 date 为字符串，防止 JSON 序列化问题
                safe_vals = []
                for v in unique_vals:
                    if hasattr(v, 'isoformat'):
                        safe_vals.append(v.isoformat())
                    else:
                        safe_vals.append(str(v))
                sample_column_values[col] = safe_vals
                
        except Exception as e:
            logger.error(f"Error processing data for chart: {e}")
            return {"chart_spec": None}

        # 3. 构造 Prompt
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # 填充模板
        # 注意: Python 的 format 需要转义大括号，或者我们手动替换
        # 为了简单，我们使用 replace 或 f-string 的变体
        
        # 先简单的字符串替换
        prompt_content = CHART_GENERATION_PROMPT.format(
            chart_generation_instructions=CHART_INSTRUCTIONS
        )
        
        # 构建用户输入部分
        user_input = f"""
### INPUT ###
Question: {user_query}
SQL: {generated_sql}
Sample Data: {json.dumps(sample_data, ensure_ascii=False, default=str)}
Sample Column Values: {json.dumps(sample_column_values, ensure_ascii=False, default=str)}
Language: Chinese
Current Time: {current_time}
Custom Instruction: None
""" 
        
        messages = [
            SystemMessage(content=prompt_content),
            HumanMessage(content=user_input)
        ]
        
        try:
            # 4. 调用 LLM
            response = llm.invoke(messages)
            content = response.content
            
            # 5. 解析 JSON
            # 尝试提取 JSON 部分 (有时 LLM 会由 markdown 包裹)
            json_str = content
            if "```json" in content:
                json_str = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                json_str = content.split("```")[1].strip()
                
            result = json.loads(json_str)
            
            chart_spec = result.get("chart_schema")
            chart_type = result.get("chart_type")
            reasoning = result.get("reasoning")
            
            # 如果 schema 为空，返回 None
            if not chart_spec or chart_spec == "":
                return {"chart_spec": None}
            
            # 注入真实数据 (Feature: 直接把所有数据塞回去给 Vega 用，如果是小数据量)
            # 注意: 对于大数据量，Vega-Lite 应该引用 URL，但这里简化处理直接嵌入
            # 如果数据量太大，前端可能会卡。这里只处理小规模结果。
            if len(execution_result) < 1000:
                 chart_spec["data"] = {"values": execution_result}
            else:
                 # 限制显示数据量，防止图表过于密集
                 chart_spec["data"] = {"values": execution_result[:50]}
            
            return {
                "chart_spec": chart_spec,
                "chart_type": chart_type,
                "chart_reasoning": reasoning
            }
            
        except Exception as e:
            logger.error(f"Error generating chart: {e}")
            return {"chart_spec": None}

    return chart_generator_node
