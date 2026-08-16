# dros — Client API

A reference for applications that create `Node`s, declare `Topic`s, and wire them
together via the `Bus`. Internal bus and transport mechanics are not covered
here; see `docs/DESIGN.md` for those.

All public symbols are re-exported from the top-level package:

```python
from dros import Bus, Node, SourceNode, Topic, Message
```

`Message` is an alias for `dict[str, object]` — the shape every published
payload must take.

---

## `Bus`

The central coordinator. Owns topics, routes messages, runs node lifecycle,
and (optionally) bridges to remote processes via a `Transport`.

### Constructor

```python
Bus(transport: Transport | None = None, *, max_workers: int = 16)
```

- `transport`: `None` (default) gives a fully local bus — `NoopTransport`. Pass
  a `ServerTransport` to accept remote clients, or a `ClientTransport` to
  connect to a remote server.
- `max_workers`: size of the thread pool used to dispatch event-mode
  callbacks.

### Lifecycle

| Method | Description |
|--------|-------------|
| `start()` | Start the bus: run every node's `startup()`, begin tick timers, emit `startup`, start transport. |
| `stop()` | Emit `shutdown`, cancel timers, run every node's `shutdown()`, stop transport, drain queues. |
| `run()` | `start()` then block until `stop()` (or `KeyboardInterrupt`). Convenience for `main`. |
| `with bus:` / context manager | Calls `start()` on entry, `stop()` on exit. |

Reserved topics emitted by the bus itself: `"startup"`, `"shutdown"`,
`"tick"`. Subscribe to these like any other topic; they are never forwarded
to the transport.

### Topics

```python
bus.topic(name: str) -> Topic
bus.state_topic(name: str, *, history: int = 0) -> Topic
```

- `topic()` creates/returns an **event** topic (default). Calling with an
  existing name returns the existing topic.
- `state_topic()` creates a **state** topic that retains history.
  - `history=0` (default): keep only the latest message.
  - `history=N`: keep the last `N` messages.
  - `history=None`: unlimited history — pass explicitly by constructing the
    `Topic` if needed; `state_topic` requires an `int`.

### Pub/sub

```python
bus.publish(topic: str, message: Message) -> None
bus.subscribe(topic, callback, *, mode: "event" | "stream" = "event") -> None
bus.unsubscribe(topic, callback) -> None
bus.clear_topic_queue(topic: str) -> None
```

- `publish`: records the message on the topic (state topics update history),
  dispatches to local subscribers, and forwards to the transport (except for
  reserved topics).
- `subscribe` modes:
  - `"event"` (default): `callback(message)` is invoked on a thread pool
    worker. Callbacks may run concurrently and out of order.
  - `"stream"`: `callback(message)` is invoked on a dedicated daemon thread
    per subscription, in publish order. Back-pressures via an internal
    `queue.Queue`. Use for ordered or stateful consumers.
- `unsubscribe`: removes the matching callback. Stream subscriptions are
  drained and their thread joined.
- `clear_topic_queue`: drops pending messages from all stream subscribers
  on the given topic.

---

## `Topic`

Returned by `bus.topic()` / `bus.state_topic()`. Usually you don't
construct one directly; use the bus methods so the bus tracks it.

### Attributes

- `name: str`
- `topic_type: "event" | "state"`
- `history_limit: int | None` — only meaningful for state topics.

### Methods (state topics only)

| Method | Description |
|--------|-------------|
| `current() -> Message \| None` | Latest recorded message, or `None`. Raises `TopicTypeError` on event topics. |
| `history() -> list[Message]` | Snapshot of retained history (oldest first). Raises `TopicTypeError` on event topics. |

Event topics raise `TopicTypeError` from `current()` and `history()`.

---

## `Node`

Base class for bus-aware components. Subclass and override the hooks you
need.

### Constructor

```python
Node(bus: Bus, *, interval: float = 0.0)
```

- Registers itself with the bus.
- `interval > 0`: enables periodic `tick()` calls on a `threading.Timer`
  that auto-reschedules. `interval == 0` (default): no ticking.

### Hooks to override

| Hook | When it runs |
|------|-------------|
| `startup(self) -> None` | `Bus.start()`, in registration order, before timers start. |
| `shutdown(self) -> None` | `Bus.stop()`, in registration order. |
| `process(self, message: Message) -> None` | Default callback for `subscribe_event` / `subscribe_stream` when no callback is supplied. Override to handle messages centrally. |
| `tick(self) -> None` | Every `interval` seconds while the bus is running. Override for periodic work. Exceptions are logged and swallowed; the timer keeps rescheduling. |

### Convenience methods (delegate to the bus)

```python
node.publish(topic: str, message: Message) -> None
node.subscribe_event(topic: str, callback=None) -> None
node.subscribe_stream(topic: str, callback=None) -> None
node.clear_topic_queue(topic: str) -> None
```

- `subscribe_event` / `subscribe_stream`: if `callback` is `None`, the
  node's own `process` method is used.

### Read-only

- `node.name` — class name.
- `node.bus` — the owning bus.

---

## `SourceNode(Node)`

A `Node` that also runs a long-lived background loop. Use for sources that
produce data from I/O, sockets, hardware, etc.

### Constructor

```python
SourceNode(bus: Bus, *, interval: float = 0.0)
```

Same constructor signature as `Node`; `interval` controls ticking as usual.

### Hook to override

```python
def run(self) -> None: ...
```

Called repeatedly on a daemon thread started in `startup()`. The loop
continues until `shutdown()` clears the run flag; exceptions in `run()` are
logged and swallowed, and the loop continues.

> If `run()` has no blocking call or sleep, the loop will spin and consume
> CPU. Add a blocking receive or a sleep inside `run()`.

`shutdown()` clears the run flag and joins the source thread (5s timeout)
before calling `Node.shutdown()`.

---

## `Transport` (optional)

Only needed when wiring multiple processes together. Construct one and pass
to `Bus(transport=...)`:

```python
from dros import ServerTransport, ClientTransport

server = ServerTransport(host="0.0.0.0", port=8080)
bus = Bus(transport=server)

client_bus = Bus(transport=ClientTransport("http://localhost:8080"))
```

### `ServerTransport`

```python
ServerTransport(
    host: str = "0.0.0.0",
    port: int = 0,                       # 0 = ephemeral; read .port after start
    *,
    ping_timeout: float = 2.0,
    ping_interval: float = 5.0,
    static_dir: str | None = None,       # serve files / "dashboard" route
)
```

- `.port: int` — actual bound port (useful when `port=0`).
- Forwards each remote publish to other subscribed clients (never back to
  the sender) and routes the message into the local bus.

### `ClientTransport`

```python
ClientTransport(
    server_url: str,
    *,
    ping_timeout: float = 2.0,
    ping_interval: float = 5.0,
)
```

- Auto-reconnects on a daemon thread. Re-subscribes to all tracked topics
  on reconnect.
- Uses a monotonic `msg_id` dedup set to skip its own looped-back messages.

### `NoopTransport`

Default. All no-ops — a purely local bus.

---

## Exceptions

```python
from dros import BusError, TopicTypeError
```

- `BusError`: base class for dros errors.
- `TopicTypeError(BusError)`: raised when calling state-only methods
  (`current`, `history`) on an event topic.

---

## Minimal example

```python
from dros import Bus, Node

class Printer(Node):
    def __init__(self, bus):
        super().__init__(bus)
        self.subscribe_event("greeting", self.on_greeting)

    def on_greeting(self, message):
        print("got:", message)

class Greeter(SourceNode):
    def run(self):
        # ...wait on some source...
        self.publish("greeting", {"text": "hello"})

bus = Bus()
Printer(bus)
Greeter(bus)
bus.run()
```

Wiring rules of thumb:

1. Subscribe in `__init__` if ordering matters — `startup()` runs after the
   bus is constructed and `startup` messages would otherwise be missed.
2. Keep callbacks short; event callbacks run on a shared pool, stream
   callbacks block their own thread.
3. Call `bus.stop()` (or exit the `with bus:` block) to shut everything down
   cleanly.