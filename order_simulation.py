"""Deterministic, in-memory simulation of ordered events and fenced writes."""

from dataclasses import dataclass, field
from threading import Lock


STATUSES = {0: "CREATED", 1: "PAID", 2: "SHIPPED"}


@dataclass(frozen=True)
class Event:
    order_id: str
    version: int
    status: str


@dataclass
class _Order:
    version: int = 0
    status: str = "CREATED"
    token: int = 0
    pending: dict[int, Event] = field(default_factory=dict)


class OrderStore:
    """The shared storage boundary serializes token grants and event writes."""

    def __init__(self):
        self._orders: dict[str, _Order] = {}
        self._next_token = 0
        self._lock = Lock()

    def create(self, order_id: str) -> None:
        with self._lock:
            if order_id in self._orders:
                raise ValueError(f"Order already exists: {order_id}")
            self._orders[order_id] = _Order()

    def acquire_token(self, order_id: str) -> int:
        """Grant ownership and immediately fence out all previous workers."""
        with self._lock:
            order = self._orders[order_id]
            self._next_token += 1
            order.token = self._next_token
            return order.token

    def deliver(self, event: Event, token: int) -> list[str]:
        with self._lock:
            order = self._orders[event.order_id]
            prefix = f"order={event.order_id} v{event.version} {event.status}"
            # Check ownership at the resource, before any event/state mutation.
            if order.token == 0 or token != order.token:
                return [f"REJECT stale/invalid worker: {prefix}; token={token}, current={order.token}"]
            if type(event.version) is not int or STATUSES.get(event.version) != event.status:
                raise ValueError("Event version/status must match the order lifecycle")
            if event.version <= order.version:
                return [f"IGNORE duplicate: {prefix}"]
            if event.version in order.pending:
                return [f"IGNORE duplicate (already pending): {prefix}"]

            order.pending[event.version] = event
            messages = []
            while order.version + 1 in order.pending:
                next_event = order.pending.pop(order.version + 1)
                order.version = next_event.version
                order.status = next_event.status
                messages.append(f"APPLY order={event.order_id} v{order.version} {order.status}")
            if order.pending:
                messages.append(
                    f"WAIT order={event.order_id}: missing v{order.version + 1}; "
                    f"pending={sorted(order.pending)}; "
                    f"state=v{order.version} {order.status}"
                )
            return messages

    def snapshot(self, order_id: str) -> tuple[int, str, tuple[int, ...]]:
        """Return immutable state, including the versions waiting for a gap."""
        with self._lock:
            order = self._orders[order_id]
            return order.version, order.status, tuple(sorted(order.pending))


def main() -> None:
    store = OrderStore()

    def send(event: Event, token: int) -> None:
        print(f"DELIVER order={event.order_id} v{event.version} {event.status}; token={token}")
        for message in store.deliver(event, token):
            print(message)

    def show(order_id: str) -> None:
        version, status, pending = store.snapshot(order_id)
        print(f"STATE order={order_id}: v{version} {status}; pending={list(pending)}")

    print("=== 1. Out-of-order delivery: v2, v1, v2 again ===")
    store.create("ordering")
    token = store.acquire_token("ordering")
    show("ordering")
    send(Event("ordering", 2, "SHIPPED"), token)
    send(Event("ordering", 1, "PAID"), token)
    send(Event("ordering", 2, "SHIPPED"), token)
    show("ordering")

    print("\n=== 2. Missing event: only v2 arrives ===")
    store.create("missing")
    token = store.acquire_token("missing")
    send(Event("missing", 2, "SHIPPED"), token)
    show("missing")

    print("\n=== 3. Stale worker: ownership changes while A is paused ===")
    store.create("workers")
    old_token = store.acquire_token("workers")
    print(f"Worker A acquires token={old_token}, then pauses")
    new_token = store.acquire_token("workers")
    print(f"Worker B acquires newer token={new_token}")
    send(Event("workers", 1, "PAID"), new_token)
    print("Worker A resumes and attempts the next valid version")
    send(Event("workers", 2, "SHIPPED"), old_token)
    show("workers")
    print("Worker B completes the order")
    send(Event("workers", 2, "SHIPPED"), new_token)
    show("workers")


if __name__ == "__main__":
    main()
