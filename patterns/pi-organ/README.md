# Pi Organ Pattern v0.2

A minimal, source-independent pattern for letting an inbound event request work while retaining enough local state to retry safely.

**Scope of v0.2:** this release provides a reusable state-transition core and a bounded local subprocess reference adapter. v0.1 contained only the core; v0.2 adds a runnable local connection example. It does not reproduce a transport poller, an agent runtime, or an end-to-end SMS self-wake deployment. The SMS field receipt remains situated evidence rather than a third-party reproduction claim.

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

`event_id` must be stable across source replays. Outbound, invalid, or malformed records are ignored. The store:

1. atomically persists new events in `pending` before wake;
2. calls `wake(InboundEvent)` in queue order;
3. moves an event to `handled` only after the callback returns;
4. retains the failed event and all later events if a callback raises;
5. deduplicates across new store instances.

State replacement fsyncs the file and containing directory and uses mode `0600` where POSIX permissions are available. Malformed state stops processing rather than resetting history. `baseline()` marks pre-existing records handled without wake. `inspect_new()` is read-only.

Failure receipts contain only `source`, a SHA-256 hash of `event_id`, and the exception type. They exclude event content and sender metadata. `source` must be a non-sensitive category such as `sms`, not an address, account, or sender identifier.

This is **at-least-once**, not exactly-once. If a process or machine fails after the callback causes an external side effect but before the handled-state commit, that event can be delivered again. Consumers should be idempotent or use their own stable-ID transaction.

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
- Repeated failures create repeated body-free receipts without a retention policy.
- The core itself imposes no content-size limit. Because `dispatch()` enqueues before calling the adapter, the adapter limit prevents child launch but does not prevent oversized durable state; sources must enforce any storage bound before enqueue.
- Discarding stdout and stderr prevents output retention but also removes diagnostics. Add only bounded, content-free observability for a real deployment.
- A source label and an “untrusted” label do not create a security boundary.
- Source polling latency, runtime availability, return-channel success, and end-to-end SMS behavior are outside this package.

## Test

From the repository root:

```sh
python3 -m unittest discover -v -s patterns/pi-organ -p 'test*.py'
```
