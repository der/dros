from __future__ import annotations

import threading
from collections import deque
from typing import Literal

from dros._exceptions import TopicTypeError
from dros._logging import logger

type TopicType = Literal["event", "state"]


class Topic:
    def __init__(
        self,
        name: str,
        *,
        topic_type: TopicType = "event",
        history_limit: int | None = None,
    ) -> None:
        self.name = name
        self.topic_type: TopicType = topic_type
        self.history_limit = history_limit
        if topic_type == "state":
            if history_limit is None:
                self._history: deque[dict[str, object]] = deque()
            else:
                self._history = deque(maxlen=max(history_limit, 1))
        self._lock = threading.Lock()

    def record(self, message: dict[str, object]) -> None:
        if self.topic_type == "state":
            with self._lock:
                self._history.append(message)
                logger.debug("Topic %s recorded message (history size=%d)", self.name, len(self._history))

    def current(self) -> dict[str, object] | None:
        if self.topic_type != "state":
            raise TopicTypeError(
                f"Topic {self.name} is type 'event', not 'state'; cannot query current()"
            )
        with self._lock:
            return self._history[-1] if self._history else None

    def history(self) -> list[dict[str, object]]:
        if self.topic_type != "state":
            raise TopicTypeError(
                f"Topic {self.name} is type 'event', not 'state'; cannot query history()"
            )
        with self._lock:
            return list(self._history)