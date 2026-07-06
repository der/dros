import threading
import time
from collections.abc import Generator
from typing import Any

import pytest

from dros import Bus
from dros._transport import ClientTransport, ServerTransport, Transport


@pytest.fixture
def server_bus() -> Generator[Bus, Any, Any]:
    transport = ServerTransport(
        host="127.0.0.1", port=0, ping_timeout=1, ping_interval=1
    )
    bus = Bus(transport=transport, max_workers=4)
    bus.start()
    yield bus
    bus.stop()


def _make_client_bus(port: int) -> Bus:
    transport = ClientTransport(
        f"http://127.0.0.1:{port}", ping_timeout=1, ping_interval=1
    )
    bus = Bus(transport=transport, max_workers=4)
    bus.start()
    return bus


def _wait_connected(transport: Transport, timeout: float = 3.0) -> None:
    assert isinstance(transport, ClientTransport)
    assert transport._connected.wait(timeout=timeout)


class TestClient:
    def test_client_publish_to_server(self, server_bus: Bus) -> None:
        server = server_bus
        received: list[dict[str, Any]] = []
        event = threading.Event()

        def callback(m: dict[str, Any]) -> None:
            received.append(m)
            event.set()

        server.subscribe("sensors", callback, mode="event")

        port = server._transport.port
        client = _make_client_bus(port)
        _wait_connected(client._transport)

        client.publish("sensors", {"temp": 42.0})
        assert event.wait(timeout=3)
        assert received == [{"temp": 42.0}]
        client.stop()

    def test_server_publish_to_client(self, server_bus: Bus) -> None:
        server = server_bus
        received: list[dict[str, Any]] = []
        event = threading.Event()

        port = server._transport.port
        client = _make_client_bus(port)
        _wait_connected(client._transport)

        def callback(m: dict[str, Any]) -> None:
            received.append(m)
            event.set()

        client.subscribe("sensors", callback, mode="event")
        time.sleep(0.2)

        server.publish("sensors", {"humidity": 55.0})
        assert event.wait(timeout=3)
        assert received == [{"humidity": 55.0}]
        client.stop()

    def test_no_loopback(self, server_bus: Bus) -> None:
        server = server_bus
        received: list[dict[str, Any]] = []
        count = 0
        event = threading.Event()

        port = server._transport.port
        client = _make_client_bus(port)
        _wait_connected(client._transport)

        def callback(m: dict[str, Any]) -> None:
            nonlocal count
            count += 1
            received.append(m)
            if count >= 1:
                event.set()

        client.subscribe("echo", callback, mode="event")
        time.sleep(0.2)

        client.publish("echo", {"ping": 1})
        time.sleep(0.3)
        assert event.wait(timeout=3)
        assert received == [{"ping": 1}]
        client.stop()

    def test_reconnect_resubscribes(self, server_bus: Bus) -> None:
        server = server_bus
        received: list[dict[str, Any]] = []
        event = threading.Event()

        port = server._transport.port
        client = _make_client_bus(port)
        _wait_connected(client._transport)

        def callback(m: dict[str, Any]) -> None:
            received.append(m)
            event.set()

        client.subscribe("data", callback, mode="event")
        time.sleep(0.2)

        server.publish("data", {"seq": 1})
        assert event.wait(timeout=3)
        assert received == [{"seq": 1}]

        event.clear()

        server_old = server
        server_old.stop()

        time.sleep(0.5)

        server_new_transport = ServerTransport(
            host="127.0.0.1", port=port, ping_timeout=1, ping_interval=1
        )
        server_new = Bus(transport=server_new_transport, max_workers=4)
        server_new.start()

        _wait_connected(client._transport, timeout=5.0)
        time.sleep(0.5)

        server_new.publish("data", {"seq": 2})
        assert event.wait(timeout=5)
        assert received == [{"seq": 1}, {"seq": 2}]

        server_new.stop()
        client.stop()