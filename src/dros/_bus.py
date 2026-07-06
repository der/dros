from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import TracebackType
from typing import Literal

from dros._logging import logger
from dros._topic import Topic
from dros._transport import NoopTransport, Transport

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
        transport: Transport | None = None,
        *,
        max_workers: int = 16,
    ) -> None:
        self._transport = transport if transport is not None else NoopTransport()
        self._transport.set_on_publish(self._on_transport_publish)

        self._topics: dict[str, Topic] = {}
        self._local_subs: dict[str, list[_EventSub | _StreamSub]] = {}
        self._nodes: list[object] = []
        self._lock = threading.RLock()
        self._running = threading.Event()
        self._shutdown_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._msg_counter = 0

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
        self._transport.subscribe(topic)

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
                    break
        self._transport.unsubscribe(topic)

    def publish(self, topic: str, message: dict[str, object]) -> None:
        with self._lock:
            self._msg_counter += 1
            msg_id = self._msg_counter
            t = self.topic(topic)
            t.record(message)
            local_subs = list(self._local_subs.get(topic, []))
        for sub in local_subs:
            if isinstance(sub, _EventSub):
                self._executor.submit(self._safe_call, sub.callback, message, topic)
            else:
                sub.queue.put(message)
        if topic not in self.RESERVED_TOPICS:
            self._transport.publish(topic, message, msg_id)

    def _on_transport_publish(
        self, topic: str, message: dict[str, object], msg_id: int
    ) -> None:
        with self._lock:
            self.topic(topic)
            local_subs = list(self._local_subs.get(topic, []))
        for sub in local_subs:
            if isinstance(sub, _EventSub):
                self._executor.submit(self._safe_call, sub.callback, message, topic)
            else:
                sub.queue.put(message)

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

        self._transport.start()

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

        self._transport.stop()

        self._executor.shutdown(wait=True)
        self._shutdown_event.set()
        logger.info("Bus stopped")

    def run(self) -> None:
        self.start()
        try:
            self._shutdown_event.wait()
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