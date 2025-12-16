"""
意图分类提示词

用于识别用户查询的意图类型：
- simple_query: 简单数据查询
- metric_query: 涉及指标体系的查询  
- metric_definition: 询问指标定义
- chitchat: 闲聊/帮助
"""

INTENT_CLASSIFIER_PROMPT = """你是一个专业的意图分类助手，负责准确分析用户查询的意图类型。

## 系统背景

{domain_description}

{metric_definitions}

---

## 向量检索匹配到的相关指标

{matched_metrics}

---

## 意图分类定义

请将用户查询分类为以下**四种意图**之一：

### 1. `simple_query` - 简单数据查询
**特征**：查询基本信息或统计数据，不涉及指标体系

**典型示例**：
- "查询所有记录"
- "有多少条数据"
- "某个字段的值是什么"

### 2. `metric_query` - 指标体系查询
**特征**：涉及指标体系的数据查询、分析或对比

**典型示例**：
- "指标情况怎么样"
- "某个指标的数值是多少"
- "对比两个对象的指标得分"

### 3. `metric_definition` - 指标定义查询
**特征**：询问指标的含义、定义、组成或评估标准

**典型示例**：
- "什么是某某指标"
- "指标包含哪些内容"
- "解释一下某个指标"

### 4. `chitchat` - 闲聊/帮助
**特征**：问候、帮助请求、与数据库无关的问题

**典型示例**：
- "你好"
- "帮助"
- "你能做什么"

---

## 用户查询

{user_query}

---

## 输出要求

请以**严格的 JSON 格式**输出，包含以下字段：

```json
{{
    "intent_type": "simple_query|metric_query|metric_definition|chitchat",
    "confidence": 0.0-1.0,
    "analysis": "你的分析理由（简洁说明为什么这样分类）",
    "identified_metrics": ["识别到的相关指标名称列表"]
}}
```

### 注意事项
1. `confidence` 应反映你对分类结果的确信程度
2. 如果查询同时涉及多个意图，选择**主要意图**
3. `identified_metrics` 只在 `metric_query` 或 `metric_definition` 时填写
4. 确保输出是合法的 JSON 格式
"""
