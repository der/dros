# Dave's ROS (dros)

An in-memory event bus with built-in socket.io support for robotics messaging.
Inspired by ROS2 and [dfdx labs](https://dfdxlabs.com/research/2026/robotics-setup/#software-setup).

## Features

- **In-process event bus** — nodes publish and subscribe to named topics
- **Stream and event subscribers** — queue-backed daemon threads or direct (thread pool) callbacks
- **State topics** — retain the last N messages, queryable without subscribing
- **Socket.io hub** — server mode for remote clients, client mode to connect to a remote hub
- **Tick scheduling** — optional per-node recurring timer callbacks
- **Source nodes** — long-running processing threads that publish messages in a loop
- **Thread-safe** — all shared state guarded by `RLock`, callbacks run outside locks
- **No async** — threading throughout, compatible with threaded I/O (audio, cameras)

## Install

```bash
uv sync --all-extras --dev
```

Requires Python ≥ 3.12. Only runtime dependency is `python-socketio`.

## Quick Start

### Local Bus

```python
from dros import Bus, Node, SourceNode

class SensorNode(SourceNode):
    def __init__(self, bus):
        super().__init__(bus, interval=0.1)
        self.subscribe_event("cmd/vel", self.on_cmd)

    def run(self):
        """Polls sensor hardware and publishes readings."""
        reading = read_sensor()
        self.publish("sensors/lidar", {"range": reading})

    def on_cmd(self, msg):
        ...

bus = Bus()
SensorNode(bus)

with bus:
    bus.publish("cmd/vel", {"v": 1.0})
```

### Server Hub

```python
from dros import Bus, ServerTransport

transport = ServerTransport(host="0.0.0.0", port=8765)
bus = Bus(transport=transport)
# register nodes...
bus.start()
# remote clients can now connect
```

### Client Hub

```python
from dros import Bus, ClientTransport, Node

class LoggerNode(Node):
    def on_sensor(self, msg):
        print(f"Sensor reading: {msg}")

transport = ClientTransport("http://192.168.1.50:8765")
bus = Bus(transport=transport)
node = LoggerNode(bus)
node.subscribe_event("sensors/lidar", node.on_sensor)
bus.start()
# receives messages from remote hub
```

## Main Classes

### Bus

Central event bus. Owns topics, routes messages, manages node lifecycle, delegates socket.io to a transport.

```python
bus = Bus()                                    # local only
bus = Bus(transport=ServerTransport(...))       # socket.io server
bus = Bus(transport=ClientTransport(...))       # socket.io client
```

### Node

Base class for nodes with lifecycle hooks and tick scheduling.

```python
class MyNode(Node):
    def __init__(self, bus):
        super().__init__(bus, interval=0.0)  # interval=0 = no tick
        self.name       # str, defaults to class name

    def startup(self) -> None: ...
    def shutdown(self) -> None: ...
    def tick(self) -> None: ...           # called at interval
    def process(self, msg) -> None: ...   # default callback

    self.subscribe_stream(topic, callback)
    self.subscribe_event(topic, callback)
    self.publish(topic, message)
```

### SourceNode

A `Node` subclass that runs a processing thread in the background. Override `run()` to poll hardware, generate messages, or run any continuous loop. The thread starts on `bus.start()` and stops on `bus.stop()`.

```python
class CameraNode(SourceNode):
    def run(self) -> None:
        frame = camera.capture()
        self.publish("camera/frame", {"data": frame})
```

### Transport

Socket.io transport layer. Three implementations:

| Class | Purpose |
|-------|---------|
| `NoopTransport` | Local-only (default when no transport given) |
| `ServerTransport(host, port, *, ping_timeout, ping_interval)` | Socket.io server hub via WSGI |
| `ClientTransport(server_url, *, ping_timeout, ping_interval)` | Socket.io client, auto-reconnects |

Custom transports can extend `Transport` ABC:

```python
class Transport(ABC):
    def publish(self, topic, message, msg_id) -> None: ...
    def subscribe(self, topic) -> None: ...
    def unsubscribe(self, topic) -> None: ...
    def set_on_publish(self, callback) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...
```

### Topic

Implicitly created by subscribe or publish. Two types:

- **event** (default) — fire and forget, no state kept
- **state** — retains the most recent message(s) for querying

```python
bus.state_topic("robot/pose", history=10)

bus.publish("robot/pose", {"x": 1.0, "y": 2.0})
bus.topic("robot/pose").current()   # -> {"x": 1.0, "y": 2.0}
bus.topic("robot/pose").history()   # -> list of last 10
```

### Messages

Messages are dicts with string keys and values compatible with socket.io which includes byte[], string and ints.

## API Summary

```python
# Bus
bus = Bus(transport=None, *, max_workers=16)
bus.topic(name)                     # -> Topic (auto-create event topic)
bus.state_topic(name, history=0)    # -> Topic (create state topic)
bus.subscribe(topic, callback, *, mode="event" | "stream")
bus.unsubscribe(topic, callback)
bus.publish(topic, message)
bus.register_node(node)
bus.start()
bus.stop()
bus.run()                           # start() + block until KeyboardInterrupt
with bus: ...                       # context manager (auto start/stop)

# Node
node.name
node.startup() / node.shutdown() / node.tick() / node.process(msg)
node.subscribe_stream(topic, callback)
node.subscribe_event(topic, callback)
node.publish(topic, message)

# SourceNode (extends Node)
node.run()                         # override: called repeatedly in a background thread

# Transport
transport = NoopTransport()
transport = ServerTransport(host="0.0.0.0", port=8765, *, ping_timeout=5, ping_interval=25)
transport = ClientTransport("http://host:port", *, ping_timeout=5, ping_interval=25)
transport.port          # ServerTransport only: actual bound port
transport.start() / transport.stop()
```

## Socket.IO Protocol

| Client → Server | Args | Effect |
|---|---|---|
| `subscribe` | `topic: str` | Subscribe to topic |
| `unsubscribe` | `topic: str` | Unsubscribe from topic |
| `publish` | `{topic, message, msg_id}` | Publish message to topic |

| Server → Client | Args | Effect |
|---|---|---|
| `publish` | `{topic, message, msg_id}` | Message broadcast on subscribed topic |

Message deduplication uses a monotonic `msg_id` assigned by the Bus. Client transports skip messages with IDs they recently sent, preventing loopback when a client both publishes and subscribes to the same topic.

## Development

```bash
uv pip install -e ".[dev]"
uv run ruff check src/
uv run pyright src/
uv run pytest
```

See `docs/DESIGN.md` for architecture details and `AGENTS.md` for development guidance.
