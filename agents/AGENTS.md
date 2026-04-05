# AGENTS DIRECTORY

## OVERVIEW
Workflow node implementations live here. Each file usually exports one `create_*` factory consumed by `graph.py`.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Intent routing | `intent_classifier.py` | First semantic split of user requests |
| Ambiguity loop | `ambiguity_checker.py` | Clarification gating for metric queries |
| Query planning | `query_planner.py` | Produces `query_plan`, `reasoning_plan`, metric selection |
| Prompt assembly | `context_assembler.py` | Builds SQL-generation context for VALUE_QUERY and schema context handoff |
| SQL generation | `sql_generator.py` | Model invocation + SQL cleanup |
| SQL execution / correction | `sql_executor.py`, `sql_corrector.py` | Runtime DB path and retry loop |
| Metric loop path | `metric_loop_planner.py`, `metric_sql_generator.py`, `metric_executor.py`, `metric_observer.py` | Iterative plan/execute/observe loop with step-level retry |
| Final UX output | `response_generator.py`, `question_suggester.py`, `chart_generator.py` | Natural-language response, recommendations, charts |

## CONVENTIONS
- Keep one node responsibility per file; return state deltas as plain dicts.
- Preserve `current_node` updates; API streaming depends on node names.
- Prefer reading/writing existing `AgentState` keys over inventing ad-hoc payload shapes.
- Error handling is mostly soft-fail: many nodes return error strings in state instead of raising.
- Most modules depend on prompt helpers or schema loaders at function scope; match that style unless a clear refactor is intended.

## ANTI-PATTERNS
- Do not rename node IDs casually; `graph.py` and `api.py` map specific names for routing and UI step labels.
- Do not return non-serializable objects in state unless downstream code already handles them.
- Do not bypass retry counters (`correction_count`, `retry_counters`, `loop_iteration`); loops are controlled by state fields.
- Do not assume all agents use the same model path; SQL generation and analysis nodes already diverge.

## HOTSPOTS
- `query_planner.py`: schema + metrics loading + JSON extraction from LLM output.
- `context_assembler.py`: bridge between planning output and downstream prompt/code path.
- `sql_executor.py`: runtime DB integration and temp-file behavior.
- `response_generator.py`: final formatting and serialization edge cases.

## NOTES
- `graph.py` imports this directory heavily; changes here ripple immediately into both CLI and API flows.
- `chart_generator.py` exists but is not part of the main LangGraph route defined in `graph.py`.
