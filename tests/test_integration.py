import threading
import time
from typing import Any

from dros import Bus, Node


class TestIntegration:
    def test_full_node_and_state_flow(self) -> None:
        bus = Bus()
        bus.state_topic("robot/pose", history=3)

        received_sensor: list[dict[str, Any]] = []
        sensor_got = threading.Event()
        received_cmd: list[dict[str, Any]] = []
        cmd_got = threading.Event()

        class SensorNode(Node):
            def __init__(self, bus: Bus) -> None:
                super().__init__(bus)
                self.subscribe_event("cmd/vel", self.on_cmd)

            def on_cmd(self, msg: dict[str, object]) -> None:
                self.publish("robot/pose", {"x": msg.get("v", 0), "y": 0})

        class ControllerNode(Node):
            def __init__(self, bus: Bus) -> None:
                super().__init__(bus)
                self.subscribe_event("robot/pose", self.on_pose)

            def startup(self) -> None:
                self.publish("cmd/vel", {"v": 1.0, "w": 0.0})

            def on_pose(self, msg: dict[str, object]) -> None:
                received_sensor.append(msg)
                sensor_got.set()

        class LogNode(Node):
            ticks_done = threading.Event()

            def __init__(self, bus: Bus, interval: float = 0.0) -> None:
                super().__init__(bus, interval=interval)
                self.subscribe_event("cmd/vel", self.on_cmd)

            def on_cmd(self, msg: dict[str, object]) -> None:
                received_cmd.append(msg)
                cmd_got.set()

        SensorNode(bus)
        ControllerNode(bus)
        log = LogNode(bus, interval=0.05)

        ticks: list[int] = []

        def on_tick_orig() -> None:
            ticks.append(1)
            if len(ticks) >= 2:
                log.ticks_done.set()

        log.tick = on_tick_orig  # type: ignore[method-assign]

        bus.start()

        assert sensor_got.wait(timeout=2)
        assert cmd_got.wait(timeout=2)
        assert log.ticks_done.wait(timeout=2)

        assert received_cmd == [{"v": 1.0, "w": 0.0}]
        assert received_sensor == [{"x": 1.0, "y": 0}]
        assert len(ticks) >= 2

        pose = bus.topic("robot/pose")
        assert pose.current() == {"x": 1.0, "y": 0}

        bus.stop()

    def test_stream_ordering(self) -> None:
        bus = Bus()
        received: list[int] = []
        done = threading.Event()

        def cb(msg: dict[str, object]) -> None:
            val = msg.get("i")
            if isinstance(val, int):
                received.append(val)
            if len(received) == 100:
                done.set()

        bus.subscribe("ordered", cb, mode="stream")
        bus.start()

        for i in range(100):
            bus.publish("ordered", {"i": i})

        assert done.wait(timeout=5)
        assert received == list(range(100))
        bus.stop()

    def test_concurrent_publishers(self) -> None:
        bus = Bus()
        received: list[dict[str, Any]] = []
        counter_lock = threading.Lock()
        barrier = threading.Barrier(4)

        def cb(msg: dict[str, object]) -> None:
            with counter_lock:
                received.append(msg)

        bus.subscribe("concurrent", cb, mode="event")
        bus.start()

        def publisher(start: int) -> None:
            barrier.wait()
            for i in range(50):
                bus.publish("concurrent", {"i": start + i})

        threads = [
            threading.Thread(target=publisher, args=(i * 100,))
            for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        time.sleep(0.3)
        assert len(received) == 200
        bus.stop()