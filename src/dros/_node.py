from __future__ import annotations

import threading
from collections.abc import Callable
from typing import TYPE_CHECKING

from dros._logging import logger

if TYPE_CHECKING:
    from dros._bus import Bus


class Node:
    def __init__(self, bus: Bus, *, interval: float = 0.0) -> None:
        self.bus = bus
        self.interval = interval
        self._timer: threading.Timer | None = None
        self._running = threading.Event()
        bus.register_node(self)

    @property
    def name(self) -> str:
        return self.__class__.__name__
    
    def __str__(self) -> str:
        return f"Node({self.name})"

    def startup(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def process(self, message: dict[str, object]) -> None:
        pass

    def tick(self) -> None:
        pass

    def subscribe_stream(
        self, topic: str, callback: Callable[[dict[str, object]], None] | None = None
    ) -> None:
        self.bus.subscribe(topic, callback or self.process, mode="stream")

    def subscribe_event(
        self, topic: str, callback: Callable[[dict[str, object]], None] | None = None
    ) -> None:
        self.bus.subscribe(topic, callback or self.process, mode="event")

    def publish(self, topic: str, message: dict[str, object]) -> None:
        self.bus.publish(topic, message)

    def clear_topic_queue(self, topic: str) -> None:
        self.bus.clear_topic_queue(topic)

    def _schedule_tick(self) -> None:
        if self.interval > 0:
            self._running.set()
            self._reschedule()

    def _reschedule(self) -> None:
        if not self._running.is_set():
            return
        self._timer = threading.Timer(self.interval, self._on_tick)
        self._timer.daemon = True
        self._timer.start()

    def _on_tick(self) -> None:
        try:
            self.tick()
        except Exception:
            logger.warning("Error in tick of %s", self.name, exc_info=True)
        finally:
            self._reschedule()

    def _cancel_tick(self) -> None:
        self._running.clear()
        if self._timer is not None:
            self._timer.cancel()


class SourceNode(Node):
    def __init__(self, bus: Bus, *, interval: float = 0.0) -> None:
        super().__init__(bus, interval=interval)
        self._source_running = threading.Event()
        self._source_thread: threading.Thread | None = None

    def run(self) -> None:
        pass

    def startup(self) -> None:
        super().startup()
        self._source_running.set()
        self._source_thread = threading.Thread(target=self._source_loop, daemon=True)
        self._source_thread.start()

    def shutdown(self) -> None:
        self._source_running.clear()
        if self._source_thread is not None:
            self._source_thread.join(timeout=5.0)
        super().shutdown()

    def _source_loop(self) -> None:
        while self._source_running.is_set():
            try:
                self.run()
            except Exception:
                logger.warning("Error in %s.run()", self.name, exc_info=True)