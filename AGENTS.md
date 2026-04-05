# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-12 17:50 Asia/Hong_Kong
**Commit:** `3f52027`
**Branch:** `main`

## OVERVIEW
Education-domain Text2SQL system built around LangGraph. Two user surfaces: CLI in `main.py` and FastAPI + SSE UI in `api.py`.

## STRUCTURE
```text
text2sql/
|- `main.py`           CLI entry, session loop
|- `api.py`            FastAPI server, SSE streaming, session store
|- `graph.py`          LangGraph workflow, routing, retries
|- `state.py`          TypedDict state contract, intent enum
|- `config.py`         .env-backed runtime config
|- `vector_store.py`   Metric lookup / fallback retrieval
|- `agents/`           Workflow node factories
|- `prompts/`          Prompt templates, domain rules, prompt builder
|- `tools/`            DB access + RL trajectory logging
|- `ui/`               Static frontend served by `api.py`
`- `test/`             Manual integration scripts
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| CLI behavior | `main.py` | `Text2SQLAgent`, reset flow, clarification loop |
| API behavior | `api.py` | `/api/chat`, `/api/chat/stream`, `/api/chart`, in-memory sessions |
| Workflow routing | `graph.py` | VALUE_QUERY goes through SQL path; METRIC_QUERY enters iterative metric loop after ambiguity check |
| Shared state fields | `state.py` | `AgentState` has conversation, SQL, analysis, verification fields |
| Config + paths | `config.py` | `.env` defaults, schema path, metrics path |
| Metric retrieval | `vector_store.py` | FAISS if installed, keyword fallback otherwise |
| Agent implementations | `agents/` | One factory per node: `create_*` |
| Prompt ownership | `prompts/` | Prompt modules mirror agent responsibilities |
| Database/logging helpers | `tools/` | `load_data`, logger, trajectory IDs |
| Frontend behavior | `ui/` | Static Element Plus UI, stream consumer |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `Text2SQLAgent` | class | `main.py` | CLI wrapper around compiled graph |
| `create_graph` | function | `graph.py` | Central workflow assembly and routing |
| `process_clarification` | function | `graph.py` | Resumes graph after follow-up answer |
| `AgentState` | TypedDict | `state.py` | State contract shared by all nodes |
| `get_or_create_session` | function | `api.py` | Builds per-session graph instances |
| `stream_graph_execution` | async function | `api.py` | SSE event formatter for node progress |
| `PromptBuilder` | class | `prompts/prompt_builder.py` | Central prompt composition layer |
| `MetricVectorStore` | class | `vector_store.py` | Metric search / definition lookup |

## EXECUTION FLOW
1. Entry via CLI (`main.py`) or API (`api.py`).
2. `create_graph()` wires LangGraph nodes from `agents/`.
3. `intent_classifier` routes to direct response, SQL path, or ambiguity path.
4. VALUE_QUERY path: `query_planner -> context_assembler -> sql_generator -> sql_executor -> response_generator`.
5. METRIC_QUERY path: `ambiguity_checker -> metric_loop_planner -> metric_sql_generator -> metric_executor -> metric_observer -> ... -> metric_cleanup -> response_generator`.
6. Optional `question_suggester` runs when `enable_suggestions` is true.

## CONVENTIONS
- No formal lint/format/test config. Follow existing Python style in nearby files instead of imposing new tooling style.
- Factory naming is consistent: agents export `create_*` callables; package boundaries live in `__init__.py` re-export lists.
- Prompt ownership is split by concern: every major agent has a sibling prompt module or prompt-builder path.
- Config is runtime-driven through `.env`; avoid hardcoding paths or service endpoints outside `config.py`.
- Comments and docstrings are predominantly Chinese; keep new knowledge-base text terse and repository-specific.

## ANTI-PATTERNS (THIS PROJECT)
- Do not invent additional architecture layers; most integration points are still root-level modules.
- Do not assume vector retrieval is active in runtime just because `vector_store.py` exists; `main.py` currently passes `embedding_client=None` into `create_graph()`.
- Do not replace manual test scripts with generic pytest instructions in docs unless the repo actually adds pytest.
- Do not treat `test/` as hermetic unit tests; scripts expect live DB / Ollama-style services.
- Do not fabricate agent/tool outputs. Existing agent memory explicitly forbids fake geocode/coordinates/links.

## UNIQUE STYLES
- Mixed architecture: root-level orchestration plus domain folders, not a package-first layout.
- LangGraph state is broad and mutation-heavy; many nodes communicate through optional fields instead of dedicated classes.
- METRIC_QUERY uses iterative plan-execute-observe SQL loop with step-level retries; VALUE_QUERY stays SQL-first.
- API streams human-readable step names through SSE for the frontend.

## COMMANDS
```bash
pip install -r requirements.txt
python main.py
python api.py
python test/test_load_data_robust.py
python test/llm_client.py
```

## NOTES
- `requirements.txt` is the only dependency manifest.
- No CI, no Makefile, no editorconfig, no lint config were found.
- `graph.py` and `api.py` are the highest-complexity single files; read them before broad changes.
- Best local AGENTS coverage points are `agents/`, `prompts/`, and `ui/`.
