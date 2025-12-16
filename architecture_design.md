# Text2SQL 智能体系统架构设计文档

## 1. 设计概述

本系统旨在构建一个针对“多级指标体系”数据库的高精度 Text2SQL 智能体。系统的核心痛点在于**复杂的指标意图识别**与**多级指标的精确选择**。

通过引入 **LangGraph** 构建有向无环图（实际上可能包含循环），我们将系统设计为一个具备“反思”与“澄清”能力的 Agent。关键策略是将“理解用户意图”与“生成 SQL”解耦：在生成 SQL 之前，必须先确保用户的意图在当前的指标体系下是清晰、无歧义的。

SQL 生成部分由专门针对完整 Schema 微调过的模型承担，因此本架构的重点在于**Prompt Engineering 的前置准备**——即如何生成一个包含精准意图和完整上下文的 Prompt 给微调模型。

## 2. 系统核心模块

系统主要分为三个层次：
1.  **意图理解层 (Intent Understanding Layer)**: 负责解析用户 Query，识别涉及的指标实体，检测歧义。
2.  **交互澄清层 (Clarification Layer)**: 当检测到歧义或缺少必要参数时，挂起任务，主动向用户提问。
3.  **执行层 (Execution Layer)**: 组装最终上下文，调用微调模型生成 SQL，执行并返回结果。

## 3. LangGraph 架构设计

### 3.1 状态定义 (State)

使用 `TypedDict` 定义图流转过程中的全局状态：

```python
from typing import TypedDict, List, Optional, Union, Dict, Any

class AgentState(TypedDict):
    # 消息历史，用于对话上下文
    messages: List[Any] 
    
    # 原始用户查询
    user_query: str
    
    # 识别出的意图类型: "direct_query" | "metric_analysis" | "ambiguous" | "chitchat"
    intent_type: Optional[str]
    
    # 识别出的指标列表 (e.g., [{"name": "带宽", "level": "2", "id": "net_bw"}])
    identified_metrics: List[Dict]
    
    # 缺失或模糊的信息 (e.g., ["time_range", "specific_metric_level"])
    ambiguity_details: List[str]
    
    # 最终用于生成 SQL 的澄清后意图描述
    refined_intent: str
    
    # 生成的 SQL
    generated_sql: str
    
    # SQL 执行结果
    execution_result: Any
    
    # 最终回答
    final_answer: str
```

### 3.2 节点 (Nodes)

#### A. `IntentClassifier` (意图分类与提取)
- **功能**: 分析用户输入，判断是闲聊、简单查数还是复杂分析。同时尝试利用模糊匹配（Fuzzy Matching）或向量检索（Vector Search）将用户口语映射到 `基教指标.json` 中的标准指标名。
- **输出**: 更新 `intent_type` 和 `identified_metrics`。

#### B. `AmbiguityChecker` (歧义检测 - **核心**)
- **功能**: 
    1. 检查 `identified_metrics` 是否为空或包含歧义（例如用户问“设施”，但“设施”下一级有“网络”、“终端”、“教室”，需要确认是查汇总还是查细项）。
    2. 检查是否缺少必要的过滤条件（如年份、学校范围），如果有默认值则填充，否则标记为缺失。
- **输出**: 更新 `ambiguity_details`。如果发现歧义，路由到 `ClarificationAgent`；否则路由到 `ContextAssembler`。

#### C. `ClarificationAgent` (澄清交互)
- **功能**: 根据 `ambiguity_details` 生成自然语言问题反问用户。
- **动作**: **中断执行 (Interrupt)**。将问题返回给用户，并等待用户的新输入。
- **注意**: 用户的回答将作为新消息加入 `messages`，并重新触发流程（通常回到 `IntentClassifier` 或专门的 `ResponseParser`）。

#### D. `ContextAssembler` (上下文组装)
- **功能**: 
    1. 读取 `full_schema.json` (完整 Schema)。
    2. 读取 `基教指标.json` 中相关指标的定义、权重信息（如果有）。
    3. 将 `refined_intent`（明确后的意图）与上述 JSON 组装成微调模型的输入 Prompt。
- **输出**: 准备好的 Prompt。

#### E. `FineTunedModel` (SQL 生成)
- **功能**: 调用微调后的 LLM。
- **输入**: 包含完整 Schema JSON 和 明确意图的 Prompt。
- **输出**: `generated_sql`。

#### F. `SQLExecutor` (执行与反馈)
- **功能**: 连接数据库执行 SQL。
- **输出**: `execution_result`。如果执行报错，可选择路由回 `FineTunedModel` 进行自我修正（Reflection），或直接报错。

#### G. `ResponseGenerator` (回复生成)
- **功能**: 结合用户 Query 和 SQL 执行结果，生成最终的自然语言分析或回答。

### 3.3 路由逻辑 (Edges)

1.  **Start** -> `IntentClassifier`
2.  `IntentClassifier` -> `AmbiguityChecker`
3.  `AmbiguityChecker` (Conditional Edge):
    - **IF** `intent_type` == 'chitchat' -> `ResponseGenerator` (直接回复)
    - **IF** `ambiguity_details` is not empty -> `ClarificationAgent`
    - **ELSE** -> `ContextAssembler`
4.  `ClarificationAgent` -> **END** (等待用户下一轮输入)
5.  `ContextAssembler` -> `FineTunedModel`
6.  `FineTunedModel` -> `SQLExecutor`
7.  `SQLExecutor` -> `ResponseGenerator`
8.  `ResponseGenerator` -> **END**

## 4. 关键逻辑细节：意图澄清 (Clarification)

这是系统的灵魂。我们需要构建一个“指标树”逻辑来辅助判断。

**场景示例**:
- **Schema**: 一级指标“基础设施” -> 二级指标“终端”、“带宽”。
- **用户 Query**: "帮我看看基础设施的情况。"
- **Agent 处理**:
    1. `IntentClassifier` 识别到关键词“基础设施” (一级指标)。
    2. `AmbiguityChecker` 发现这是一个父节点，且用户未指定聚合方式（是求和？还是列出所有子项？）。
    3. 系统判断存在歧义。
    4. `ClarificationAgent` 生成回复: "您是指所有的基础设施指标（终端、带宽等）的详细数据，还是需要一个综合评分？"
    5. **暂停**，等待用户回复。
    6. **用户回复**: "就要综合评分。"
    7. 系统更新 `refined_intent` 为 "计算基础设施下所有二级指标的加权综合评分"，进入 SQL 生成环节。

## 5. 微调模型交互规范

鉴于微调模型只见过完整 Schema，我们的 Prompt 策略应当是“Full Context + Explicit Instruction”。

**Prompt 模板示例**:

```json
{
  "instruction": "你是专业的 Text2SQL 助手。请根据下方提供的 Schema 和 用户的查询意图 生成准确的 SQL。",
  "schema": <full_schema.json_content>,
  "metric_context": {
      "question_context": "用户想要计算'基础设施'的综合得分。",
      "metric_definition": "基础设施 = 网络(权重0.3) + 终端(权重0.4) + 教室(权重0.3)"  // 如果需要计算，在此处注入知识
  },
  "user_query": "计算全校基础设施的综合得分"
}
```

注意：即使用户模型不需要额外操作，我们必须保证传入的 `user_query` 是经过澄清后的、包含精确指标术语的文本，而不是用户原始的模糊提问。

## 6. 总结

该架构通过 LangGraph 的显式状态管理，确保了在生成 SQL 这种昂贵且易错的操作之前，Agent 已经完全理解了用户的需求。这对于复杂的、多层级指标体系尤为重要，能够显著减少生成无效 SQL 的概率。
