# IMPORTANT! General principles

1. Don't assume. Don't hide confusion. Surface tradeoffs.
2. Minimum code that solves the problem. Nothing speculative.
3. Touch only what you must. Clean up only your own mess.
4. Define success criteria. Loop until verified.
5. If adding new libraries ensure they are not GPL.

# Project specific

- Use pyproject.toml with src directory style of layout.
- Python >= 3.12 required.
- No CI, Docker, Makefile, or pre-commit. Lint/typecheck/test are manual.

## Dev commands

```bash
uv pip install -e ".[dev]"
uv run ruff check src/       # E, F, I, UP, B, SIM; ignore E501
uv run pyright src/          # standard strictness
uv run pytest                # run all tests, expect ~4s
```

Run lint + typecheck + tests before committing.

## Architecture

See design notes in `docs/DESIGN.md`.

Modules (all under `src/dros/`):

| File | Public | Purpose |
|------|--------|---------|
| `_bus.py` | `Bus` | Event bus, pub/sub, socket.io hub, lifecycle |
| `_node.py` | `Node` | Base class for nodes with tick scheduling |
| `_topic.py` | `Topic` | Event topics (stateless) and state topics (history) |
| `_exceptions.py` | `BusError`, `TopicTypeError` | Custom exceptions |
| `_logging.py` | — | Shared `logging.getLogger("dros")` |

## Threading model

- All threading, no async. `ThreadPoolExecutor` for event callbacks, `queue.Queue` + daemon threads for stream subscribers.
- Per-node `threading.Timer` for tick scheduling (auto-reschedule, cancel on shutdown).
- Bus state guarded by `threading.RLock`. Lock held briefly for mutation; callbacks run outside the lock.

## Socket.IO notes

- Server uses `python-socketio` sync mode (`async_mode='threading'`) with a `wsgiref.simple_server` + `ThreadingMixIn` (stdlib, no extra deps beyond `python-socketio[client]` for tests).
- `wsgiref` does not support WebSocket; clients must use `transports=['polling']`.
- Default polling cycle is 25s. Tests use `ping_timeout=1, ping_interval=1` for speed.
- `Bus.stop()` calls `server.shutdown()` to stop `serve_forever()` cleanly.

## Gotchas

- `Bus.__enter__` calls `start()` — subscribe BEFORE the `with` block if you need `startup` messages.
- Node `startup()` and `shutdown()` run in order of registration. If ordering matters, subscribe in `__init__` instead.
- State topic `history_limit=None` (default) = unlimited history; `history_limit=0` = keep only latest.
- Stream subscriber threads are daemon threads and share the process lifetime.