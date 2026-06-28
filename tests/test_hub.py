import threading
import time
from collections.abc import Generator
from typing import Any

import pytest

from dros import Bus


@pytest.fixture
def bus_with_hub() -> Generator[Bus, Any, Any]:
    bus = Bus(host="127.0.0.1", port=0, max_workers=4, ping_timeout=1, ping_interval=1)
    bus.start()
    yield bus
    bus.stop()


class TestHub:
    def test_remote_publish_to_local(self, bus_with_hub: Bus) -> None:
        bus = bus_with_hub
        received: list[dict[str, Any]] = []
        event = threading.Event()

        def callback(m: dict[str, Any]) -> None:
            received.append(m)
            event.set()
        bus.subscribe("test", callback, mode="event")

        import socketio
        sio_client = socketio.Client()
        got = threading.Event()

        @sio_client.on("connect")
        def on_connect() -> None:
            got.set()

        port = bus._port
        sio_client.connect(
            f"http://127.0.0.1:{port}",
            transports=["polling"],
            wait_timeout=5,
        )
        assert got.wait(timeout=3)

        sio_client.emit("publish", {"topic": "test", "message": {"hello": "world"}})
        assert event.wait(timeout=2)
        assert received == [{"hello": "world"}]
        sio_client.disconnect()

    def test_remote_subscribe_receives_publish(self, bus_with_hub: Bus) -> None:
        bus = bus_with_hub
        import socketio
        sio_client = socketio.Client()
        received: list[dict[str, Any]] = []
        got = threading.Event()

        @sio_client.on("connect")
        def on_connect() -> None:
            got.set()

        @sio_client.on("publish")
        def on_publish(data: dict[str, Any]) -> None:
            received.append(data["message"])

        port = bus._port
        sio_client.connect(
            f"http://127.0.0.1:{port}",
            transports=["polling"],
            wait_timeout=5,
        )
        assert got.wait(timeout=3)

        sio_client.emit("subscribe", "sensors")
        time.sleep(0.1)

        bus.publish("sensors", {"temp": 25.5})
        time.sleep(0.2)
        assert received == [{"temp": 25.5}]
        sio_client.disconnect()

    def test_remote_disconnect_cleans_up(self, bus_with_hub: Bus) -> None:
        bus = bus_with_hub
        import socketio
        sio_client = socketio.Client()
        got = threading.Event()

        @sio_client.on("connect")
        def on_connect() -> None:
            got.set()

        port = bus._port
        sio_client.connect(
            f"http://127.0.0.1:{port}",
            transports=["polling"],
            wait_timeout=5,
        )
        assert got.wait(timeout=3)

        sio_client.emit("subscribe", "sensors")
        time.sleep(0.1)

        with bus._lock:
            assert "sensors" in bus._remote_subs

        sio_client.disconnect()
        time.sleep(0.1)

        with bus._lock:
            assert "sensors" not in bus._remote_subs

    def test_bus_without_hub(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        def callback(m: dict[str, Any]) -> None:
            received.append(m)
            event.set()
        bus.subscribe("test", callback, mode="event")
        bus.start()
        bus.publish("test", {"x": 1})
        assert event.wait(timeout=2)
        assert received == [{"x": 1}]
        bus.stop()