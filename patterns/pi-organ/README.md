# Pi Organ Pattern v0.3

A minimal, transport-independent pattern for letting an inbound event request work while retaining enough local state to retry safely.

**Scope of v0.3:** this release adds explicit, opt-in failure isolation between source domains to the reusable state-transition core. v0.2 added the bounded local subprocess reference adapter; that adapter still delivers one `InboundEvent` per callback invocation. This package does not provide a transport poller, an agent runtime, an OpenClaw adapter or adoption claim, or an end-to-end SMS self-wake deployment. The SMS field receipt remains situated evidence rather than a third-party reproduction claim.

## Generic reusable core

[`pi_organ.py`](pi_organ.py) accepts normalized records with:

```json
{
  "direction": "inbound",
  "valid": true,
  "event_id": "example-event-1",
  "source": "sensor.example.invalid",
  "arrival_timestamp": "2030-01-02T03:04:05Z",
  "content": "untrusted data"
}
```

`event_id` must be stable across replays and **globally unique across every source in one store**. Deduplication identity is the bare `event_id`, not `(source, event_id)`. Adapters for providers whose IDs are only locally unique must namespace them before enqueue (for example, `provider:account:local-id`). A collision from another source is treated as the already-known event; v0.3 does not change the state schema or silently reinterpret existing IDs. Outbound, invalid, or malformed records are ignored. The store:

1. atomically persists new events in `pending` before wake;
2. calls `wake(InboundEvent)` for one event at a time;
3. moves an event to `handled` only after the callback returns;
4. commits each callback success before attempting another event;
5. retains failed events for retry; and
6. deduplicates across new store instances.

State replacement fsyncs the file and containing directory and uses mode `0600` where POSIX permissions are available. Malformed state stops processing rather than resetting history. `baseline()` marks pre-existing records handled without wake. `inspect_new()` is read-only.

### Failure policies

`process_pending()` and `dispatch()` accept a keyword-only `failure_policy`:

- `failure_policy="fail_fast"` is the default and preserves v0.2 behavior. The first callback failure remains pending, all later events remain unattempted, a body-free receipt is committed, and the original callback exception is re-raised.
- `failure_policy="isolate_sources"` is explicit opt-in. A failed event remains pending and later pending events with the same exact `source` are skipped for the rest of that pass. Events from other sources continue in queue order, and each success is committed independently. At the end of a pass containing failures, `SourceIsolationError` is raised so partial progress cannot be mistaken for complete success. Its public fields and standard string representation expose only the callback failure count and the non-sensitive source categories, and it does not store original callback exceptions. Because that pass raises, its normal completed-ID return value is available only when no callback failed; `snapshot()` shows the durable handled and pending state after partial progress.

For example:

```python
from pi_organ import SourceIsolationError

try:
    store.dispatch(records, wake, failure_policy="isolate_sources")
except SourceIsolationError as error:
    # Some unrelated sources may already be durably handled.
    print(error.failure_count, error.failed_sources)
```

An invalid policy is rejected before records are consumed, callbacks run, or state is mutated. Each callback receives a JSON-deep-detached event, so mutating nested `content` cannot alter pending state or corrupt a failure commit; a retry receives the original durable content.

Isolated processing returns from its private event-bearing helper before constructing `SourceIsolationError`, which keeps those helper locals and original callback tracebacks out of the summary exception's traceback. This is deliberately narrower than claiming that arbitrary traceback introspection is content-free: callers and callback objects can retain their own sensitive references, and Python frames outside the core remain the caller's responsibility.

`source` is a **conservative ordering and failure-isolation domain**, not merely a descriptive transport label. Events that must never overtake one another must use the same exact source value. Isolation permits a later event from a different source to complete while an earlier source is blocked, so choose a broader domain whenever ordering requirements are uncertain. A source must also be a non-sensitive category such as `sms`, not an address, account, sender identifier, credential, or authorization decision. Source equality does not establish trust.

Failure receipts contain only `source`, a SHA-256 hash of `event_id`, and the exception type. They exclude event content, sender metadata, raw event IDs, and exception messages.

This is **at-least-once**, not exactly-once. If a process or machine fails after the callback causes an external side effect but before the handled-state commit, that event can be delivered again. A failed event is retried on a later pass, and every event skipped behind that source stays pending in its original order. Successfully committed events from other sources are not retried by the store. Consumers should still be idempotent or use their own stable-ID transaction because the callback side effect and local handled-state commit are not one transaction.

## Reference subprocess adapter

[`subprocess_adapter.py`](subprocess_adapter.py) is a runnable callback implementation. `SubprocessWakeAdapter`:

- accepts only a sequence of argv strings, never a shell command;
- serializes the normalized `InboundEvent` as one JSON value on child stdin;
- never adds event content or metadata to process argv;
- enforces a configurable payload limit (64 KiB by default, with a 1 MiB hard maximum);
- enforces a finite timeout (10 seconds by default, with a 5 minute hard maximum);
- discards child stdout and stderr, retaining zero output bytes;
- treats exit status zero as callback success and raises on timeout or nonzero exit;
- starts with an empty child environment unless an explicit environment mapping is supplied.

A timeout, nonzero exit, or oversized payload therefore travels through the core's normal callback-failure path: the event remains pending and can be retried. Exception messages and durable failure receipts do not include child output or event content.

Configuration example:

```python
import sys

wake = SubprocessWakeAdapter(
    [sys.executable, "path/to/trusted_consumer.py"],
    max_payload_bytes=65_536,
    timeout_seconds=10,
)
store.dispatch(records, wake)
```

Do not place credentials, event data, or other secrets in the command sequence. If a consumer needs credentials, use an appropriately protected mechanism outside argv and give the child only the minimum access it needs. Supplying `environment=` is explicit because an inherited environment can itself contain sensitive values.

## End-to-end local demo

From the repository root:

```sh
python3 patterns/pi-organ/demo.py
```

Expected output:

```text
{"handled": ["demo-event"], "pending": []}
```

The demo sends a synthetic normalized event through the durable core, into [`example_consumer.py`](example_consumer.py) over stdin, and back to a handled-state commit. The example consumer only validates the bounded JSON and exits; it performs no external action and emits no event data.

## Adapter and source contracts

The source must not irreversibly acknowledge or checkpoint an event before durable `enqueue()` succeeds, **or** it must allow replay by stable `event_id`. Otherwise a crash between source acknowledgement and pending-state persistence can lose an event.

Exit status zero only reports that the local consumer returned successfully. It does not prove recognition, a reply, transport success, or any lasting cognitive effect.

The configured executable is inside the trust boundary. It receives untrusted event content and can copy, log, execute, or disclose it. This adapter does not sandbox the child, authenticate a sender, authorize an action, prevent prompt injection, or validate how downstream tools use content. The timeout bounds waiting for the direct child; it is not a guarantee that descendants or external side effects stop.

## Real-world evidence

The three situated trials behind this abstraction are reported in the [field receipt](../../notes/2026-09-15-pi-organ-field-receipt.md). The private field deployment is not reproduced here because its audit exposed unsafe and installation-specific coupling.

## Known limits

- The reference state writer targets POSIX-like filesystems; portability of directory fsync and permission bits is not claimed.
- The JSON store assumes one externally coordinated writer and low event volume.
- Pending state necessarily retains untrusted content locally; protect the state path and its backups.
- Atomic replacement protects each commit, not the consumer's external side effects.
- Every attempted failure creates another body-free receipt; receipts have no retention policy.
- Isolation preserves ordering only within an exact `source` value. It deliberately allows work in another source domain to pass a failure.
- The core itself imposes no content-size limit. Because `dispatch()` enqueues before calling the adapter, the adapter limit prevents child launch but does not prevent oversized durable state; sources must enforce any storage bound before enqueue.
- Discarding stdout and stderr prevents output retention but also removes diagnostics. Add only bounded, content-free observability for a real deployment.
- A source label and an “untrusted” label do not create a security boundary.
- Source polling latency, runtime availability, return-channel success, and end-to-end SMS behavior are outside this package.

## Test

From the repository root:

```sh
python3 -m unittest discover -v -s patterns/pi-organ -p 'test*.py'
```
