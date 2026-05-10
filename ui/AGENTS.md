# UI DIRECTORY

## OVERVIEW
Static frontend served by `api.py`. This layer is thin: HTML shell, browser logic, and CSS for the FastAPI-backed chat interface.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Markup structure | `index.html` | Element Plus-based layout, message panes, toggles |
| Login page | `login.html` | Login/signup UI with illustration |
| Client behavior | `script.js` | Stream consumption, login state, history sync, chart/result rendering |
| Visual styling | `style.css` | Theme tokens, layout, component look |
| Login styling | `login_splash.css` | Login/signup page styles |

## CONVENTIONS
- Keep API assumptions aligned with SSE payloads from `api.py` (`start`, `step`, `result`, `error`, `[DONE]`).
- UI is Chinese-language and dashboard-like; preserve labels and mental model.
- Frontend is static-file based, not a bundler app; avoid introducing build-step expectations casually.
- Logged-in mode uses bearer auth plus separate history endpoints; local anonymous mode still uses browser-only thread snapshots.

## ANTI-PATTERNS
- Do not treat this as a componentized SPA; there is no React/Vue build pipeline here.
- Do not rename fields from backend event payloads without updating both `api.py` and `script.js`.
- Do not add asset pipeline assumptions, npm commands, or module imports unless the repo structure changes first.
- Do not send logged-in chat traffic to a custom conversation streaming endpoint. The intended flow is `/api/chat/stream` for inference, then `/api/conversations/{id}/messages` for persistence.

## NOTES
- `index.html` pulls Element Plus from CDN.
- The sidebar history is server-backed when the user is logged in, and local-only when the user is anonymous.
- SQL, Python code, and suggested follow-up questions can all appear in result payloads; keep rendering tolerant of optional fields.
- Frontend changes are safest when verified against `/api/chat/stream` plus the history endpoints:
  - `GET /api/conversations`
  - `GET /api/conversations/{id}/messages`
  - `POST /api/conversations/{id}/messages`
