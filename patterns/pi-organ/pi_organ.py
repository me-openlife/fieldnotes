"""Small durable bridge from normalized inbound events to a wake callback.

This core remains transport- and runtime-independent. Event content is
untrusted data; the package's reference subprocess adapter passes it on stdin
rather than placing it in process arguments.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Iterable, Mapping


class StateError(RuntimeError):
    """The durable state could not be trusted, so processing stopped."""


@dataclass(frozen=True)
class InboundEvent:
    """A source-independent event. ``content`` must be treated as untrusted."""

    event_id: str
    source: str
    arrival_timestamp: str
    content: Any


def normalize_inbound(record: object) -> InboundEvent | None:
    """Validate one normalized record, ignoring outbound or invalid records.

    Adapters should supply ``direction='inbound'`` and ``valid=True``. Unknown,
    malformed, and outbound records return ``None`` rather than entering state.
    """
    if not isinstance(record, Mapping):
        return None
    if record.get("direction") != "inbound" or record.get("valid") is not True:
        return None
    values = []
    for key in ("event_id", "source", "arrival_timestamp"):
        value = record.get(key)
        if not isinstance(value, str) or not value.strip():
            return None
        values.append(value)
    if "content" not in record:
        return None
    try:
        # State is JSON, so reject values that could never be durably enqueued.
        json.dumps(record["content"], ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return None
    return InboundEvent(values[0], values[1], values[2], record["content"])


class PiOrganStore:
    """Atomic JSON state for a single low-volume producer/consumer.

    The store is intentionally single-writer. Coordinate externally if several
    processes can mutate the same path.
    """

    VERSION = 1

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {"version": 1, "pending": [], "handled": [], "failures": []}

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._empty()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StateError("durable state is unreadable or malformed") from exc
        if not isinstance(data, dict) or data.get("version") != self.VERSION:
            raise StateError("unsupported or malformed durable state")
        if not all(isinstance(data.get(key), list) for key in ("pending", "handled", "failures")):
            raise StateError("malformed durable state collections")
        if not all(isinstance(item, str) and item for item in data["handled"]):
            raise StateError("malformed handled IDs")
        if len(set(data["handled"])) != len(data["handled"]):
            raise StateError("duplicate handled IDs")
        pending: list[dict[str, Any]] = data["pending"]
        seen: set[str] = set()
        for item in pending:
            if not isinstance(item, dict) or set(item) != {
                "event_id", "source", "arrival_timestamp", "content"
            }:
                raise StateError("malformed pending event")
            event = normalize_inbound({**item, "direction": "inbound", "valid": True})
            if event is None or event.event_id in seen:
                raise StateError("malformed or duplicate pending event")
            seen.add(event.event_id)
        if seen.intersection(data["handled"]):
            raise StateError("an event cannot be both pending and handled")
        for item in data["failures"]:
            if not isinstance(item, dict) or set(item) != {"source", "event_id_hash", "error_type"}:
                raise StateError("malformed failure receipt")
            if not all(isinstance(value, str) and value for value in item.values()):
                raise StateError("malformed failure receipt values")
        return data

    def _save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, ensure_ascii=False, allow_nan=False, indent=2) + "\n"
        fd, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        try:
            os.chmod(temporary, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            # The rename itself is not durable until the containing directory is.
            directory_fd = os.open(
                self.path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass

    def baseline(self, records: Iterable[object]) -> list[str]:
        """Mark pre-existing inbound IDs handled without invoking a callback."""
        state = self._load()
        handled = set(state["handled"])
        added: list[str] = []
        for record in records:
            event = normalize_inbound(record)
            if event is not None and event.event_id not in handled:
                handled.add(event.event_id)
                added.append(event.event_id)
        state["pending"] = [item for item in state["pending"] if item["event_id"] not in handled]
        state["handled"] = sorted(handled)
        self._save(state)
        return added

    def inspect_new(self, records: Iterable[object]) -> list[InboundEvent]:
        """Return valid unseen candidates without changing state or waking."""
        state = self._load()
        known = set(state["handled"]) | {item["event_id"] for item in state["pending"]}
        result: list[InboundEvent] = []
        for record in records:
            event = normalize_inbound(record)
            if event is not None and event.event_id not in known:
                result.append(event)
                known.add(event.event_id)
        return result

    def enqueue(self, records: Iterable[object]) -> list[InboundEvent]:
        """Durably add valid unseen inbound events before any wake is attempted."""
        state = self._load()
        known = set(state["handled"]) | {item["event_id"] for item in state["pending"]}
        added: list[InboundEvent] = []
        for record in records:
            event = normalize_inbound(record)
            if event is None or event.event_id in known:
                continue
            state["pending"].append(asdict(event))
            known.add(event.event_id)
            added.append(event)
        if added:
            self._save(state)
        return added

    def process_pending(self, wake: Callable[[InboundEvent], None]) -> list[str]:
        """Process pending events in order, stopping at the first callback error.

        A successful callback is committed as handled before the next event.
        A failure remains pending, as do all later events, and the exception is
        re-raised after a body-free failure receipt is durably recorded.
        """
        state = self._load()
        completed: list[str] = []
        while state["pending"]:
            item = state["pending"][0]
            event = InboundEvent(**item)
            try:
                wake(event)
            except Exception as exc:
                state["failures"].append(
                    {
                        "source": event.source,
                        "event_id_hash": hashlib.sha256(event.event_id.encode("utf-8")).hexdigest(),
                        "error_type": type(exc).__name__,
                    }
                )
                self._save(state)
                raise
            state["pending"].pop(0)
            if event.event_id not in state["handled"]:
                state["handled"].append(event.event_id)
                state["handled"].sort()
            self._save(state)
            completed.append(event.event_id)
        return completed

    def dispatch(self, records: Iterable[object], wake: Callable[[InboundEvent], None]) -> list[str]:
        """Enqueue all records durably, then process the pending queue."""
        self.enqueue(records)
        return self.process_pending(wake)

    def snapshot(self) -> dict[str, Any]:
        """Return a detached state snapshot for local health inspection."""
        return json.loads(json.dumps(self._load(), ensure_ascii=False))
