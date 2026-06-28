import threading

import pytest

from dros import Topic, TopicTypeError


class TestEventTopic:
    def test_implicitly_event_type(self) -> None:
        t = Topic("test")
        assert t.topic_type == "event"
        assert t.name == "test"

    def test_record_is_noop(self) -> None:
        t = Topic("test")
        t.record({"key": "value"})

    def test_current_raises(self) -> None:
        t = Topic("test")
        with pytest.raises(TopicTypeError, match="event"):
            t.current()

    def test_history_raises(self) -> None:
        t = Topic("test")
        with pytest.raises(TopicTypeError, match="event"):
            t.history()


class TestStateTopic:
    def test_creates_state_topic(self) -> None:
        t = Topic("sensors", topic_type="state")
        assert t.topic_type == "state"

    def test_current_returns_none_when_empty(self) -> None:
        t = Topic("sensors", topic_type="state")
        assert t.current() is None

    def test_current_returns_latest(self) -> None:
        t = Topic("sensors", topic_type="state")
        t.record({"val": 1})
        t.record({"val": 2})
        assert t.current() == {"val": 2}

    def test_history_returns_all_recorded(self) -> None:
        t = Topic("sensors", topic_type="state")
        t.record({"a": 1})
        t.record({"b": 2})
        assert t.history() == [{"a": 1}, {"b": 2}]

    def test_history_limit(self) -> None:
        t = Topic("sensors", topic_type="state", history_limit=2)
        t.record({"a": 1})
        t.record({"b": 2})
        t.record({"c": 3})
        assert t.history() == [{"b": 2}, {"c": 3}]
        assert t.current() == {"c": 3}

    def test_history_limit_zero_keeps_one(self) -> None:
        t = Topic("sensors", topic_type="state", history_limit=0)
        t.record({"a": 1})
        t.record({"b": 2})
        assert t.current() == {"b": 2}
        assert len(t.history()) == 1
        assert t.history() == [{"b": 2}]

    def test_thread_safety(self) -> None:
        t = Topic("concurrent", topic_type="state", history_limit=100)
        barrier = threading.Barrier(4)
        errors: list[Exception] = []

        def record_messages(start: int) -> None:
            try:
                barrier.wait()
                for i in range(100):
                    t.record({"idx": start + i})
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=record_messages, args=(i * 100,))
            for i in range(4)
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors
        assert t.current() is not None
        assert len(t.history()) == 100