from __future__ import annotations

import contextlib
import threading
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from dros._logging import logger

if TYPE_CHECKING:
    import socketio


class Transport(ABC):
    @abstractmethod
    def publish(self, topic: str, message: dict[str, object], msg_id: int) -> None: ...

    @abstractmethod
    def subscribe(self, topic: str) -> None: ...

    @abstractmethod
    def unsubscribe(self, topic: str) -> None: ...

    @abstractmethod
    def set_on_publish(
        self, callback: Callable[[str, dict[str, object], int], None]
    ) -> None: ...

    @abstractmethod
    def start(self) -> None: ...

    @abstractmethod
    def stop(self) -> None: ...


class NoopTransport(Transport):
    def publish(self, topic: str, message: dict[str, object], msg_id: int) -> None:
        pass

    def subscribe(self, topic: str) -> None:
        pass

    def unsubscribe(self, topic: str) -> None:
        pass

    def set_on_publish(
        self, callback: Callable[[str, dict[str, object], int], None]
    ) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class ServerTransport(Transport):
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        *,
        ping_timeout: float = 2.0,
        ping_interval: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._ping_timeout = ping_timeout
        self._ping_interval = ping_interval
        self._on_publish_handler: Callable[[str, dict[str, object], int], None] | None = (
            None
        )
        self._remote_subs: dict[str, set[str]] = {}
        self._sid_to_topics: dict[str, set[str]] = {}
        self._sio: socketio.Server | None = None
        self._wsgi_server: Any = None
        self._wsgi_thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return self._port

    def set_on_publish(
        self, callback: Callable[[str, dict[str, object], int], None]
    ) -> None:
        self._on_publish_handler = callback

    def publish(self, topic: str, message: dict[str, object], msg_id: int) -> None:
        for sid in self._remote_subs.get(topic, set()):
            if self._sio is not None:
                self._sio.emit(
                    "publish",
                    {"topic": topic, "message": message, "msg_id": msg_id},
                    to=sid,
                )

    def subscribe(self, topic: str) -> None:
        pass

    def unsubscribe(self, topic: str) -> None:
        pass

    def start(self) -> None:
        import socketio

        self._sio = socketio.Server(
            async_mode="threading",
            ping_timeout=self._ping_timeout,
            ping_interval=self._ping_interval,
        )
        assert self._sio is not None
        self._sio.on("connect", self._on_connect)
        self._sio.on("disconnect", self._on_disconnect)
        self._sio.on("subscribe", self._on_remote_subscribe)
        self._sio.on("unsubscribe", self._on_remote_unsubscribe)
        self._sio.on("publish", self._on_remote_publish)

        from werkzeug.serving import make_server

        app = socketio.WSGIApp(self._sio)
        self._wsgi_server = make_server(
            self._host,
            self._port,
            app,
            threaded=True,
        )
        self._port = self._wsgi_server.server_port
        self._wsgi_thread = threading.Thread(
            target=self._wsgi_server.serve_forever, daemon=True
        )
        self._wsgi_thread.start()
        logger.info("Socket.IO server listening on %s:%d", self._host, self._port)

    def stop(self) -> None:
        if self._sio is not None:
            for sid in list(self._sid_to_topics):
                with contextlib.suppress(Exception):
                    self._sio.disconnect(sid)
        if self._wsgi_thread is not None and self._wsgi_thread.is_alive():
            self._wsgi_server.shutdown()
            self._wsgi_thread.join(timeout=2.0)
        if self._wsgi_server is not None:
            self._wsgi_server.server_close()
        self._remote_subs.clear()
        self._sid_to_topics.clear()
        self._sio = None

    def _on_connect(self, sid: str, environ: dict[str, object]) -> None:
        self._sid_to_topics[sid] = set()
        logger.info("Socket.IO client connected: %s", sid)

    def _on_disconnect(self, sid: str) -> None:
        topics = self._sid_to_topics.pop(sid, set())
        for topic in topics:
            sids = self._remote_subs.get(topic)
            if sids is not None:
                sids.discard(sid)
                if not sids:
                    del self._remote_subs[topic]
        logger.info("Socket.IO client disconnected: %s", sid)

    def _on_remote_subscribe(self, sid: str, topic: str) -> None:
        self._remote_subs.setdefault(topic, set()).add(sid)
        self._sid_to_topics.setdefault(sid, set()).add(topic)
        logger.debug("Remote %s subscribed to %s", sid, topic)

    def _on_remote_unsubscribe(self, sid: str, topic: str) -> None:
        sids = self._remote_subs.get(topic)
        if sids is not None:
            sids.discard(sid)
            if not sids:
                del self._remote_subs[topic]
        self._sid_to_topics.get(sid, set()).discard(topic)
        logger.debug("Remote %s unsubscribed from %s", sid, topic)

    def _on_remote_publish(self, sid: str, data: dict[str, object]) -> None:
        topic = data.get("topic")
        message = data.get("message")
        msg_id_raw = data.get("msg_id", 0)
        msg_id = int(msg_id_raw) if isinstance(msg_id_raw, (int, float)) else 0

        if not (isinstance(topic, str) and isinstance(message, dict)):
            logger.warning("Invalid publish from %s: %s", sid, data)
            return

        for other_sid in self._remote_subs.get(topic, set()):
            if other_sid != sid and self._sio is not None:
                self._sio.emit(
                    "publish",
                    {"topic": topic, "message": message, "msg_id": msg_id},
                    room=other_sid,
                )

        if self._on_publish_handler is not None:
            self._on_publish_handler(topic, message, msg_id)


class ClientTransport(Transport):
    def __init__(
        self,
        server_url: str,
        *,
        ping_timeout: float = 2.0,
        ping_interval: float = 5.0,
    ) -> None:
        self._server_url = server_url
        self._ping_timeout = ping_timeout
        self._ping_interval = ping_interval
        self._on_publish_handler: Callable[[str, dict[str, object], int], None] | None = (
            None
        )
        self._topics: set[str] = set()
        self._sent_ids: deque[int] = deque(maxlen=100)
        self._client: socketio.Client | None = None
        self._connected = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def set_on_publish(
        self, callback: Callable[[str, dict[str, object], int], None]
    ) -> None:
        self._on_publish_handler = callback

    def publish(self, topic: str, message: dict[str, object], msg_id: int) -> None:
        with self._lock:
            self._sent_ids.append(msg_id)
        if self._connected.is_set():
            assert self._client is not None
            self._client.emit(
                "publish",
                {"topic": topic, "message": message, "msg_id": msg_id},
            )

    def subscribe(self, topic: str) -> None:
        with self._lock:
            self._topics.add(topic)
        if self._connected.is_set():
            assert self._client is not None
            self._client.emit("subscribe", topic)

    def unsubscribe(self, topic: str) -> None:
        with self._lock:
            self._topics.discard(topic)
        if self._connected.is_set():
            assert self._client is not None
            self._client.emit("unsubscribe", topic)

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._connect_and_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._client is not None:
            with contextlib.suppress(Exception):
                self._client.disconnect()
        self._connected.clear()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None
        with self._lock:
            self._topics.clear()
            self._sent_ids.clear()

    def _connect_and_loop(self) -> None:
        import time as _time

        import socketio

        client = socketio.Client()
        self._client = client

        def _on_connect() -> None:
            with self._lock:
                topics_to_subscribe = list(self._topics)
            self._connected.set()
            self._sent_ids.clear()
            logger.info("Client connected to %s", self._server_url)
            for topic in topics_to_subscribe:
                client.emit("subscribe", topic)

        client.on("connect", _on_connect, namespace="/")

        def _on_disconnect() -> None:
            self._connected.clear()
            logger.info("Client disconnected from %s", self._server_url)

        client.on("disconnect", _on_disconnect, namespace="/")

        def _on_publish(data: dict[str, object]) -> None:
            topic = data.get("topic")
            message = data.get("message")
            msg_id_raw = data.get("msg_id", 0)
            msg_id = int(msg_id_raw) if isinstance(msg_id_raw, (int, float)) else 0
            if not (isinstance(topic, str) and isinstance(message, dict)):
                return
            with self._lock:
                if msg_id in self._sent_ids:
                    return
            if self._on_publish_handler is not None:
                self._on_publish_handler(topic, message, msg_id)

        client.on("publish", _on_publish, namespace="/")

        while not self._stop.is_set():
            try:
                client.connect(
                    self._server_url,
                    transports=["websocket"],
                    wait_timeout=5,
                )
                while not self._stop.is_set() and client.connected:
                    client.sleep(0.5)  # type: ignore[reportArgumentType]
            except Exception:
                logger.warning(
                    "Connection to %s failed, retrying...",
                    self._server_url,
                )
                if not self._stop.is_set():
                    _time.sleep(2.0)
            finally:
                with contextlib.suppress(Exception):
                    client.disconnect()
                self._connected.clear()
