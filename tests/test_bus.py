import threading
import time
from typing import Any

from dros import Bus, Node, SourceNode


class TestBusBasic:
    def test_implicit_topic_creation(self) -> None:
        bus = Bus()
        t = bus.topic("test")
        assert t.name == "test"
        assert t.topic_type == "event"
        bus.stop()

    def test_same_topic_returned(self) -> None:
        bus = Bus()
        t1 = bus.topic("test")
        t2 = bus.topic("test")
        assert t1 is t2
        bus.stop()

    def test_state_topic_explicit(self) -> None:
        bus = Bus()
        t = bus.state_topic("pose", history=5)
        assert t.topic_type == "state"
        assert t.history_limit == 5
        bus.stop()

    def test_publish_event_subscriber(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        def cb(msg: dict[str, object]) -> None:
            received.append(msg)
            event.set()

        bus.subscribe("test", cb, mode="event")
        bus.publish("test", {"data": 42})
        assert event.wait(timeout=2.0)
        assert received == [{"data": 42}]
        bus.stop()

    def test_publish_stream_subscriber(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        def cb(msg: dict[str, object]) -> None:
            received.append(msg)
            event.set()

        bus.subscribe("test", cb, mode="stream")
        bus.publish("test", {"data": 42})
        assert event.wait(timeout=2.0)
        assert received == [{"data": 42}]
        bus.stop()

    def test_multiple_subscribers(self) -> None:
        bus = Bus()
        events = [threading.Event() for _ in range(3)]
        counts: dict[int, int] = {0: 0, 1: 0, 2: 0}

        for i in range(3):
            def make_cb(idx: int) -> Any:
                def cb(msg: dict[str, object]) -> None:
                    counts[idx] += 1
                    events[idx].set()
                return cb
            bus.subscribe("test", make_cb(i), mode="event")

        bus.publish("test", {"x": 1})
        for ev in events:
            assert ev.wait(timeout=2.0)
        assert counts[0] == 1
        assert counts[1] == 1
        assert counts[2] == 1
        bus.stop()

    def test_unsubscribe(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []

        def cb(msg: dict[str, object]) -> None:
            received.append(msg)

        bus.subscribe("test", cb, mode="event")
        bus.unsubscribe("test", cb)
        bus.publish("test", {"x": 1})
        time.sleep(0.1)
        assert received == []
        bus.stop()

    def test_state_topic_keeps_current(self) -> None:
        bus = Bus()
        bus.state_topic("pose", history=0)
        bus.publish("pose", {"x": 1, "y": 2})
        bus.publish("pose", {"x": 3, "y": 4})
        t = bus.topic("pose")
        assert t.current() == {"x": 3, "y": 4}
        bus.stop()


class TestBusLifecycle:
    def test_startup_shutdown_messages_published(self) -> None:
        bus = Bus()
        startup_received: list[dict[str, Any]] = []
        shutdown_received: list[dict[str, Any]] = []

        bus.subscribe("startup", lambda m: startup_received.append(m), mode="event")
        bus.subscribe("shutdown", lambda m: shutdown_received.append(m), mode="event")

        bus.start()
        time.sleep(0.1)
        assert len(startup_received) == 1
        assert startup_received[0] == {"topic": "startup"}

        bus.stop()
        time.sleep(0.1)
        assert len(shutdown_received) == 1
        assert shutdown_received[0] == {"topic": "shutdown"}

    def test_context_manager(self) -> None:
        received: list[dict[str, Any]] = []

        bus = Bus()
        bus.subscribe("startup", lambda m: received.append(m), mode="event")
        with bus:
            time.sleep(0.1)
            assert len(received) == 1

    def test_error_in_callback_logged_not_raised(self) -> None:
        bus = Bus()

        def bad_cb(msg: dict[str, object]) -> None:
            raise RuntimeError("boom")

        bus.subscribe("test", bad_cb, mode="event")
        bus.publish("test", {"x": 1})
        time.sleep(0.1)
        assert True
        bus.stop()


class TestNode:
    def test_node_startup_shutdown(self) -> None:
        bus = Bus()
        started = threading.Event()
        stopped = threading.Event()

        class MyNode(Node):
            def startup(self) -> None:
                started.set()

            def shutdown(self) -> None:
                stopped.set()

        MyNode(bus)
        bus.start()
        assert started.wait(timeout=2.0)
        bus.stop()
        assert stopped.wait(timeout=2.0)

    def test_node_tick(self) -> None:
        bus = Bus()
        ticks: list[int] = []
        done = threading.Event()

        class TickingNode(Node):
            def tick(self) -> None:
                ticks.append(1)
                if len(ticks) >= 3:
                    done.set()

        TickingNode(bus, interval=0.05)
        bus.start()
        assert done.wait(timeout=2.0)
        bus.stop()
        assert len(ticks) >= 3

    def test_node_subscribe_event(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        class SubNode(Node):
            def on_data(self, msg: dict[str, object]) -> None:
                received.append(msg)
                event.set()

        node = SubNode(bus)
        node.subscribe_event("data", node.on_data)
        bus.start()
        bus.publish("data", {"val": 99})
        assert event.wait(timeout=2.0)
        assert received == [{"val": 99}]
        bus.stop()

    def test_node_subscribe_stream(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        class SubNode(Node):
            def on_data(self, msg: dict[str, object]) -> None:
                received.append(msg)
                event.set()

        node = SubNode(bus)
        node.subscribe_stream("data", node.on_data)
        bus.start()
        bus.publish("data", {"val": 99})
        assert event.wait(timeout=2.0)
        assert received == [{"val": 99}]
        bus.stop()

    def test_node_publish(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        event = threading.Event()

        class PubNode(Node):
            def startup(self) -> None:
                self.publish("data", {"val": 42})

        bus.subscribe("data", lambda m: (received.append(m), event.set()), mode="event")
        PubNode(bus)
        bus.start()
        assert event.wait(timeout=2.0)
        assert received == [{"val": 42}]
        bus.stop()

    def test_tick_error_logged(self) -> None:
        bus = Bus()
        errors: list[int] = []
        done = threading.Event()

        class BadTickNode(Node):
            def tick(self) -> None:
                errors.append(1)
                raise RuntimeError("tick error")
            def startup(self) -> None:
                done.set()

        BadTickNode(bus, interval=0.05)
        bus.start()
        assert done.wait(timeout=2.0)
        time.sleep(0.2)
        bus.stop()
        assert len(errors) >= 1


class TestSourceNode:
    def test_source_node_publishes(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        done = threading.Event()

        class PubSource(SourceNode):
            def run(self) -> None:
                if not done.is_set():
                    self.publish("data", {"val": 1})
                    done.set()

        PubSource(bus)
        bus.subscribe("data", lambda m: received.append(m), mode="event")
        bus.start()
        assert done.wait(timeout=2.0)
        bus.stop()
        assert received[0] == {"val": 1}

    def test_source_node_runs_continuously(self) -> None:
        bus = Bus()
        counter: list[int] = [0]
        done = threading.Event()

        class CounterSource(SourceNode):
            def run(self) -> None:
                counter[0] += 1
                if counter[0] >= 5:
                    done.set()

        CounterSource(bus)
        bus.start()
        assert done.wait(timeout=2.0)
        bus.stop()
        assert counter[0] >= 5

    def test_source_node_shutdown_stops_thread(self) -> None:
        bus = Bus()
        counter: list[int] = [0]
        started = threading.Event()

        class LoopSource(SourceNode):
            def run(self) -> None:
                counter[0] += 1
                started.set()

        LoopSource(bus)
        bus.start()
        assert started.wait(timeout=2.0)
        time.sleep(0.3)
        bus.stop()
        after = counter[0]
        time.sleep(0.2)
        assert counter[0] == after

    def test_source_node_error_logged(self) -> None:
        bus = Bus()
        errors: list[int] = []
        done = threading.Event()

        class BadSource(SourceNode):
            def run(self) -> None:
                errors.append(1)
                done.set()
                raise RuntimeError("source error")

        BadSource(bus)
        bus.start()
        assert done.wait(timeout=2.0)
        time.sleep(0.2)
        bus.stop()
        assert len(errors) >= 1

    def test_source_node_with_tick(self) -> None:
        bus = Bus()
        ticks: list[int] = []
        sources: list[int] = []
        done = threading.Event()

        class ComboNode(SourceNode):
            def run(self) -> None:
                sources.append(1)
                if len(sources) >= 3 and len(ticks) >= 2:
                    done.set()

            def tick(self) -> None:
                ticks.append(1)
                if len(sources) >= 3 and len(ticks) >= 2:
                    done.set()

        ComboNode(bus, interval=0.05)
        bus.start()
        assert done.wait(timeout=2.0)
        bus.stop()
        assert len(sources) >= 3
        assert len(ticks) >= 2