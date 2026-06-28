# Dave's ROS (dros)

An in-memory event bus with built-in socket.io support for robotics messaging.
Inspired by [dfdx labs](https://dfdxlabs.com/research/2026/robotics-setup/#software-setup).

## Features

- **In-process event bus** — nodes publish and subscribe to named topics
- **Stream and event subscribers** — queue-backed daemon threads or ThreadPoolExecutor callbacks
- **State topics** — retain the last N messages, queryable without subscribing
- **Socket.io hub** — remote clients connect, subscribe, and publish bidirectionally
- **Tick scheduling** — per-node recurring timer callbacks
- **Thread-safe** — all shared state guarded by `RLock`, callbacks run outside locks
- **No async** — threading throughout, compatible with threaded I/O (audio, cameras)

## Install

```bash
pip install -e .
```

Requires Python ≥ 3.12. Only runtime dependency is `python-socketio`.

## Main Classes

### Bus

The central event bus. Owns topics, routes messages, manages node lifecycle, runs the socket.io server.

```python
from dros import Bus

# local-only
bus = Bus()
bus.start()

# with socket.io hub
bus = Bus(host="0.0.0.0", port=8765)
bus.run()  # start() + block until KeyboardInterrupt
```

### Node

Base class for nodes. Hooks for `startup()`, `shutdown()`, `tick()`, and `process()`.

```python
from dros import Node

class SensorNode(Node):
    def __init__(self, bus):
        super().__init__(bus, interval=0.1)  # tick every 100ms
        self.subscribe_event("cmd/vel", self.on_cmd)

    def on_cmd(self, msg):
        self.publish("sensors/lidar", {"range": 1.5})

    def tick(self):
        self.publish("heartbeat", {"node": self.name})
```

### Topic

Implicitly created by subscribe or publish. Two types:

- **event** (default) — fire and forget, no state kept
- **state** — retains the most recent message(s) for querying

```python
bus.state_topic("robot/pose", history=10)  # keep last 10

bus.publish("robot/pose", {"x": 1.0, "y": 2.0})
pose = bus.topic("robot/pose").current()   # -> {"x": 1.0, "y": 2.0}
history = bus.topic("robot/pose").history() # -> list of last 10
```

## API Summary

```python
# Bus
bus = Bus(host=None, port=None, *, max_workers=16, ping_timeout=5, ping_interval=25)
bus.topic(name)                    # -> Topic (auto-create event topic)
bus.state_topic(name, history=0)   # -> Topic (create state topic)
bus.subscribe(topic, callback, *, mode="event" | "stream")
bus.unsubscribe(topic, callback)
bus.publish(topic, message)
bus.register_node(node)
bus.start()                        # publish startup, launch WSGI, start ticks
bus.stop()                         # publish shutdown, cancel ticks, shutdown server
bus.run()                          # start() + block until interrupt
with bus: ...                      # context manager (auto start/stop)

# Node
class MyNode(Node):
    def __init__(self, bus, *, interval=0.0): ...
    name        # str, defaults to class name
    startup()   # called on bus.start()
    shutdown()  # called on bus.stop()
    tick()      # called at interval (if interval > 0)
    process(msg)  # default callback
    subscribe_stream(topic, callback)
    subscribe_event(topic, callback)
    publish(topic, message)

# Topic
t = Topic(name, *, topic_type="event" | "state", history_limit=None)
t.record(message)    # store if state topic
t.current()          # -> Message | None (state only)
t.history()          # -> list[Message] (state only)
```

## Socket.IO Protocol

Remote clients connect to the bus hub. Supported events:

| Client → Server | Args | Effect |
|---|---|---|
| `subscribe` | `topic: str` | Subscribe to topic |
| `unsubscribe` | `topic: str` | Unsubscribe from topic |
| `publish` | `{topic, message}` | Publish message to topic |

| Server → Client | Args | Effect |
|---|---|---|
| `publish` | `{topic, message}` | Message broadcast on subscribed topic |

## Development

```bash
uv pip install -e ".[dev]"
uv run ruff check src/
uv run pyright src/
uv run pytest
```

See `docs/DESIGN.md` for architecture details and `AGENTS.md` for development guidance.
