from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from dros._logging import logger
from dros._topic import Topic

if TYPE_CHECKING:
    import socketio

_SubMode = Literal["event", "stream"]


class _EventSub:
    __slots__ = ("topic", "callback")

    def __init__(self, topic: str, callback: Callable[[dict[str, object]], None]) -> None:
        self.topic = topic
        self.callback = callback


class _StreamSub:
    __slots__ = ("topic", "callback", "queue", "thread", "running")

    def __init__(self, topic: str, callback: Callable[[dict[str, object]], None]) -> None:
        self.topic = topic
        self.callback = callback
        self.queue: queue.Queue[dict[str, object] | None] = queue.Queue()
        self.running = threading.Event()
        self.running.set()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self) -> None:
        while self.running.is_set():
            try:
                msg = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if msg is None:
                break
            self._safe_call(msg)

    def _safe_call(self, msg: dict[str, object]) -> None:
        try:
            self.callback(msg)
        except Exception:
            logger.warning(
                "Error in stream subscriber for topic %s", self.topic, exc_info=True
            )

    def stop(self) -> None:
        self.running.clear()
        self.queue.put(None)
        self.thread.join(timeout=2.0)


class Bus:
    RESERVED_TOPICS = ("startup", "shutdown", "tick")

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        *,
        max_workers: int = 16,
        ping_timeout: float = 2.0,
        ping_interval: float = 5.0,
    ) -> None:
        self._topics: dict[str, Topic] = {}
        self._local_subs: dict[str, list[_EventSub | _StreamSub]] = {}
        self._remote_subs: dict[str, set[str]] = {}
        self._sid_to_topics: dict[str, set[str]] = {}
        self._nodes: list[object] = []
        self._lock = threading.RLock()
        self._running = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

        self._host = host
        self._port = port
        self._ping_timeout = ping_timeout
        self._ping_interval = ping_interval
        self._sio: socketio.Server | None = None
        self._wsgi_server: Any = None
        self._wsgi_thread: threading.Thread | None = None

        for name in self.RESERVED_TOPICS:
            self._topics[name] = Topic(name, topic_type="event")

    def topic(self, name: str) -> Topic:
        with self._lock:
            if name not in self._topics:
                self._topics[name] = Topic(name, topic_type="event")
                logger.debug("Implicitly created event topic %s", name)
            return self._topics[name]

    def state_topic(self, name: str, *, history: int = 0) -> Topic:
        with self._lock:
            t = Topic(name, topic_type="state", history_limit=history)
            self._topics[name] = t
            logger.info("Created state topic %s (history=%d)", name, history)
            return t

    def subscribe(
        self,
        topic: str,
        callback: Callable[[dict[str, object]], None],
        *,
        mode: _SubMode = "event",
    ) -> None:
        with self._lock:
            self.topic(topic)
            if topic not in self._local_subs:
                self._local_subs[topic] = []
            if mode == "event":
                sub: _EventSub | _StreamSub = _EventSub(topic, callback)
            else:
                sub = _StreamSub(topic, callback)
            self._local_subs[topic].append(sub)
            logger.debug("Subscribed to %s (mode=%s)", topic, mode)

    def unsubscribe(
        self, topic: str, callback: Callable[[dict[str, object]], None]
    ) -> None:
        with self._lock:
            subs = self._local_subs.get(topic, [])
            for sub in subs:
                if sub.callback is callback:
                    subs.remove(sub)
                    if isinstance(sub, _StreamSub):
                        sub.stop()
                    logger.debug("Unsubscribed from %s", topic)
                    return

    def publish(self, topic: str, message: dict[str, object]) -> None:
        with self._lock:
            t = self.topic(topic)
            t.record(message)
            local_subs = list(self._local_subs.get(topic, []))
            remote_sids = set(self._remote_subs.get(topic, set()))
        for sub in local_subs:
            if isinstance(sub, _EventSub):
                self._executor.submit(self._safe_call, sub.callback, message, topic)
            else:
                sub.queue.put(message)
        for sid in remote_sids:
            if self._sio is not None:
                self._sio.emit(
                    "publish",
                    {"topic": topic, "message": message},
                    room=sid,
                )

    @staticmethod
    def _safe_call(
        callback: Callable[[dict[str, object]], None],
        message: dict[str, object],
        topic: str,
    ) -> None:
        try:
            callback(message)
        except Exception:
            logger.warning(
                "Error in event subscriber for topic %s", topic, exc_info=True
            )

    def register_node(self, node: object) -> None:
        with self._lock:
            self._nodes.append(node)

    def start(self) -> None:
        self._running.set()

        for node in self._nodes:
            if hasattr(node, "startup"):
                try:
                    node.startup()  # type: ignore[union-attr]
                except Exception:
                    logger.warning("Error in startup of %s", node, exc_info=True)  # type: ignore[union-attr]

        for node in self._nodes:
            if hasattr(node, "_schedule_tick"):
                node._schedule_tick()  # type: ignore[union-attr]

        self.publish("startup", {"topic": "startup"})

        if self._host is not None and self._port is not None:
            self._start_wsgi()

        logger.info("Bus started")

    def stop(self) -> None:
        logger.info("Bus stopping...")
        self._running.clear()

        self.publish("shutdown", {"topic": "shutdown"})

        for node in self._nodes:
            if hasattr(node, "_cancel_tick"):
                node._cancel_tick()  # type: ignore[union-attr]

        for node in self._nodes:
            if hasattr(node, "shutdown"):
                try:
                    node.shutdown()  # type: ignore[union-attr]
                except Exception:
                    logger.warning("Error in shutdown of %s", node, exc_info=True)  # type: ignore[union-attr]

        with self._lock:
            for subs in self._local_subs.values():
                for sub in list(subs):
                    if isinstance(sub, _StreamSub):
                        sub.stop()
            self._local_subs.clear()
            self._remote_subs.clear()
            self._sid_to_topics.clear()

        if self._wsgi_thread is not None and self._wsgi_thread.is_alive():
            self._wsgi_server.shutdown()
            self._wsgi_thread.join(timeout=2.0)

        self._executor.shutdown(wait=True)
        logger.info("Bus stopped")

    def run(self) -> None:
        self.start()
        try:
            self._running.wait()
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def __enter__(self) -> Bus:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.stop()

    def _start_wsgi(self) -> None:
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

        from socketserver import ThreadingMixIn
        from wsgiref.simple_server import WSGIServer, make_server

        class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
            daemon_threads = True

        assert self._host is not None and self._port is not None
        app = socketio.WSGIApp(self._sio)
        self._wsgi_server = make_server(
            self._host, self._port, app, server_class=ThreadingWSGIServer
        )
        self._port = self._wsgi_server.server_port
        self._wsgi_thread = threading.Thread(
            target=self._wsgi_server.serve_forever, daemon=True
        )
        self._wsgi_thread.start()
        logger.info("Socket.IO server listening on %s:%d", self._host, self._port)

    def _on_connect(self, sid: str, environ: dict[str, object]) -> None:
        with self._lock:
            self._sid_to_topics[sid] = set()
        logger.info("Socket.IO client connected: %s", sid)

    def _on_disconnect(self, sid: str) -> None:
        with self._lock:
            topics = self._sid_to_topics.pop(sid, set())
            for topic in topics:
                sids = self._remote_subs.get(topic)
                if sids is not None:
                    sids.discard(sid)
                    if not sids:
                        del self._remote_subs[topic]
        logger.info("Socket.IO client disconnected: %s", sid)

    def _on_remote_subscribe(self, sid: str, topic: str) -> None:
        with self._lock:
            self.topic(topic)
            self._remote_subs.setdefault(topic, set()).add(sid)
            self._sid_to_topics.setdefault(sid, set()).add(topic)
        logger.debug("Remote %s subscribed to %s", sid, topic)

    def _on_remote_unsubscribe(self, sid: str, topic: str) -> None:
        with self._lock:
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
        if isinstance(topic, str) and isinstance(message, dict):
            self.publish(topic, message)
        else:
            logger.warning("Invalid publish from %s: %s", sid, data)