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


class SourceIsolationError(RuntimeError):
    """Summarize failures from an isolated processing pass.

    Its public attributes and string representation contain only a count and
    source categories. The exception does not store callback exceptions or
    event identifiers.
    """

    def __init__(self, failure_count: int, failed_sources: Iterable[str]):
        self.failure_count = failure_count
        self.failed_sources = tuple(dict.fromkeys(failed_sources))
        count_label = "failure" if failure_count == 1 else "failures"
        sources = ", ".join(self.failed_sources)
        super().__init__(
            f"{failure_count} callback {count_label} in source categories: {sources}"
        )


@dataclass(frozen=True)
class InboundEvent:
    """A source-independent event. ``content`` must be treated as untrusted."""

    event_id: str
    source: str
    arrival_timestamp: str
    content: Any


@dataclass(frozen=True)
class _ProcessingSuccess:
    completed: tuple[str, ...]


@dataclass(frozen=True)
class _IsolationFailure:
    failure_count: int
    failed_sources: tuple[str, ...]


_ProcessOutcome = _ProcessingSuccess | _IsolationFailure


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

    @staticmethod
    def _validate_failure_policy(failure_policy: str) -> None:
        if not isinstance(failure_policy, str) or failure_policy not in {
            "fail_fast",
            "isolate_sources",
        }:
            raise ValueError(
                "failure_policy must be 'fail_fast' or 'isolate_sources'"
            )

    @staticmethod
    def _append_failure_receipt(
        state: dict[str, Any], event: InboundEvent, error_type: str
    ) -> None:
        state["failures"].append(
            {
                "source": event.source,
                "event_id_hash": hashlib.sha256(event.event_id.encode("utf-8")).hexdigest(),
                "error_type": error_type,
            }
        )

    @staticmethod
    def _detached_callback_event(item: dict[str, Any]) -> InboundEvent:
        """Copy already-validated JSON content before giving it to a callback."""
        content = json.loads(
            json.dumps(item["content"], ensure_ascii=False, allow_nan=False)
        )
        return InboundEvent(
            event_id=item["event_id"],
            source=item["source"],
            arrival_timestamp=item["arrival_timestamp"],
            content=content,
        )

    def _process_pending_sensitive(
        self,
        wake: Callable[[InboundEvent], None],
        failure_policy: str,
    ) -> _ProcessOutcome:
        """Process callbacks while keeping event-bearing locals off summary traces."""
        state = self._load()
        completed: list[str] = []
        blocked_sources: set[str] = set()
        failed_sources: list[str] = []
        failure_count = 0
        index = 0

        while index < len(state["pending"]):
            item = state["pending"][index]
            durable_event = InboundEvent(**item)
            if durable_event.source in blocked_sources:
                index += 1
                continue
            callback_event = self._detached_callback_event(item)
            try:
                wake(callback_event)
            except Exception as exc:
                self._append_failure_receipt(
                    state, durable_event, type(exc).__name__
                )
                self._save(state)
                if failure_policy == "fail_fast":
                    raise
                blocked_sources.add(durable_event.source)
                failed_sources.append(durable_event.source)
                failure_count += 1
                index += 1
                continue

            state["pending"].pop(index)
            if durable_event.event_id not in state["handled"]:
                state["handled"].append(durable_event.event_id)
                state["handled"].sort()
            self._save(state)
            completed.append(durable_event.event_id)

        if failure_count:
            return _IsolationFailure(failure_count, tuple(failed_sources))
        return _ProcessingSuccess(tuple(completed))

    @staticmethod
    def _resolve_processing_outcome(outcome: _ProcessOutcome) -> list[str]:
        """Turn a content-free processing outcome into the public result."""
        if isinstance(outcome, _IsolationFailure):
            raise SourceIsolationError(
                outcome.failure_count, outcome.failed_sources
            ) from None
        return list(outcome.completed)

    def process_pending(
        self,
        wake: Callable[[InboundEvent], None],
        *,
        failure_policy: str = "fail_fast",
    ) -> list[str]:
        """Process pending events according to ``failure_policy``.

        ``fail_fast`` preserves the original behavior: processing stops at the
        first callback error and that error is re-raised. ``isolate_sources``
        retains each failed event, skips later events from the same source for
        this pass, and continues with other sources. After that pass it raises
        :class:`SourceIsolationError` if any callback failed.

        Callback events are detached from durable pending state. Every
        successful callback is committed as handled before another event is
        attempted. Every failure receipt is also committed before processing
        continues or an exception is raised.
        """
        self._validate_failure_policy(failure_policy)
        outcome = self._process_pending_sensitive(wake, failure_policy)
        # Do not retain the callback in a SourceIsolationError traceback frame.
        del wake
        return self._resolve_processing_outcome(outcome)

    def dispatch(
        self,
        records: Iterable[object],
        wake: Callable[[InboundEvent], None],
        *,
        failure_policy: str = "fail_fast",
    ) -> list[str]:
        """Enqueue records durably, then process them under ``failure_policy``."""
        # Validate before consuming records or mutating durable state.
        self._validate_failure_policy(failure_policy)
        self.enqueue(records)
        outcome = self._process_pending_sensitive(wake, failure_policy)
        # The sensitive helper has returned. Drop caller-owned event references
        # and the callback before a summary exception can be constructed.
        del records
        del wake
        return self._resolve_processing_outcome(outcome)

    def snapshot(self) -> dict[str, Any]:
        """Return a detached state snapshot for local health inspection."""
        return json.loads(json.dumps(self._load(), ensure_ascii=False))
