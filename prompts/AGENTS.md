# PROMPTS DIRECTORY

## OVERVIEW
Prompt text, SQL rules, domain definitions, and prompt-construction helpers live here. This directory mirrors agent responsibilities more than UI or transport concerns.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Intent prompt | `intent_classifier_prompt.py` | Query classification wording |
| Ambiguity prompt | `ambiguity_checker_prompt.py` | Clarification detection |
| Planner prompt | `query_planner_prompt.py` | Structured JSON plan output |
| Context assembly | `context_assembler_prompt.py` | SQL prompt framing |
| Code-analysis prompt | `data_analyzer_prompt.py` | Python-analysis generation |
| Response prompt | `response_prompt.py` | Chitchat and result phrasing |
| Suggestions | `suggestion_prompt.py` | Follow-up questions |
| SQL safety/rules | `sql_rules.py`, `sql_correction_prompt.py` | DB-specific behavior and correction prompts |
| Domain registry | `domain_config.py` | Education-specific business rules |
| Shared builder | `prompt_builder.py` | Central composition abstraction |

## CONVENTIONS
- Keep domain knowledge here, not inside agent control flow, when the behavior is prompt-shaped.
- Existing structure prefers one concern per file plus `__init__.py` re-exports for shared imports.
- Education-domain defaults are first-class; preserve that assumption unless multi-domain support is explicitly expanded.
- Prompt outputs often expect machine-readable JSON or SQL; wording changes can break parsers downstream.

## ANTI-PATTERNS
- Do not add generic prompting advice that duplicates what the model already knows; keep constraints task-shaped.
- Do not change output formats without checking the parsing logic in sibling agents.
- Do not hardcode schema snippets in multiple files when `PromptBuilder` or domain config can own them once.
- Do not repeat parent-level architecture notes here; focus on prompt contracts and domain rules.

## NOTES
- `domain_config.py` contains project-specific ambiguity rules and business assumptions for education metrics.
- `sql_samples.py` and `sql_rules.py` are support modules, not standalone entry points.
- If behavior changes only when wording changes, start in this directory before editing agent code.
