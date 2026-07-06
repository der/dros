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
| `_bus.py` | `Bus` | Event bus, pub/sub, lifecycle |
| `_node.py` | `Node` | Base class for nodes with tick scheduling |
| `_topic.py` | `Topic` | Event topics (stateless) and state topics (history) |
| `_transport.py` | `Transport`, `NoopTransport`, `ServerTransport`, `ClientTransport` | Socket.io transport layer |
| `_exceptions.py` | `BusError`, `TopicTypeError` | Custom exceptions |
| `_logging.py` | — | Shared `logging.getLogger("dros")` |

## Transport module (`_transport.py`)

- `Transport` is an ABC with 6 methods: `publish(topic, msg, msg_id)`, `subscribe(topic)`, `unsubscribe(topic)`, `set_on_publish(callback)`, `start()`, `stop()`.
- `NoopTransport`: all no-ops. Default when `Bus(transport=None)`.
- `ServerTransport`: socket.io server + WSGI thread. Tracks `_remote_subs` (topic→sids) and `_sid_to_topics` (sid→topics). Remote publish forwards to other sids (excludes sender), then calls bus handler for local routing.
- `ClientTransport`: socket.io client with auto-reconnect. Tracks `_topics` for re-subscription on reconnect. Uses monotonic `msg_id` dedup (`_sent_ids` set) to skip loopbacks.
- Bus assigns monotonic `_msg_counter` to each `publish()`. `msg_id` is transport metadata, never injected into the message dict. Wire format is `{"topic", "message", "msg_id"}`.
- Reserved topics (`startup`, `shutdown`, `tick`) are NOT forwarded to transport — local only.

## Threading model

- All threading, no async. `ThreadPoolExecutor` for event callbacks, `queue.Queue` + daemon threads for stream subscribers.
- Per-node `threading.Timer` for tick scheduling (auto-reschedule, cancel on shutdown).
- Bus state guarded by `threading.RLock`. Lock held briefly for mutation; callbacks run outside the lock.
- Server transport runs WSGI in a daemon thread; client transport runs the connect loop in a daemon thread.

## Socket.IO notes

- Server uses `python-socketio` sync mode (`async_mode='threading'`) with a `wsgiref.simple_server` + `ThreadingMixIn` (stdlib, no extra deps beyond `python-socketio[client]` for tests).
- `wsgiref` does not support WebSocket; clients must use `transports=['polling']`.
- Default polling cycle is 25s. Tests use `ping_timeout=1, ping_interval=1` for speed.
- `Bus.stop()` calls `transport.stop()` — server does `server.shutdown()` + `server_close()`; client does `disconnect()` + thread join.

## Gotchas

- `Bus.__enter__` calls `start()` — subscribe BEFORE the `with` block if you need `startup` messages.
- Node `startup()` and `shutdown()` run in order of registration. If ordering matters, subscribe in `__init__` instead.
- State topic `history_limit=None` (default) = unlimited history; `history_limit=0` = keep only latest.
- Stream subscriber threads are daemon threads and share the process lifetime.
- Client transport emits `subscribe`/`unsubscribe` only when connected. Topics tracked in `_topics` are re-subscribed on reconnect.
- Server transport forwards remote publishes to other sids but NOT back to the sender (no echo).
- 