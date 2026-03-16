# UI DIRECTORY

## OVERVIEW
Static frontend served by `api.py`. This layer is thin: HTML shell, browser logic, and CSS for the FastAPI-backed chat interface.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Markup structure | `index.html` | Element Plus-based layout, message panes, toggles |
| Client behavior | `script.js` | Stream consumption, chat state, chart/result rendering |
| Visual styling | `style.css` | Theme tokens, layout, component look |

## CONVENTIONS
- Keep API assumptions aligned with SSE payloads from `api.py` (`start`, `step`, `result`, `error`, `[DONE]`).
- UI is Chinese-language and dashboard-like; preserve labels and mental model.
- Frontend is static-file based, not a bundler app; avoid introducing build-step expectations casually.

## ANTI-PATTERNS
- Do not treat this as a componentized SPA; there is no React/Vue build pipeline here.
- Do not rename fields from backend event payloads without updating both `api.py` and `script.js`.
- Do not add asset pipeline assumptions, npm commands, or module imports unless the repo structure changes first.

## NOTES
- `index.html` pulls Element Plus from CDN.
- The sidebar history is partly mock/static UI, not fully persisted server state.
- SQL, Python code, and suggested follow-up questions can all appear in result payloads; keep rendering tolerant of optional fields.
- Frontend changes are safest when verified against `/api/chat/stream`, not just the non-streaming endpoint.
