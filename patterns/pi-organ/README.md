# Pi Organ Pattern v0.1

A minimal, source-independent pattern for letting an inbound event request work while retaining enough local state to retry safely.

**Scope of v0.1:** this release claims a reusable state-transition core, not a third-party-reproducible SMS self-wake deployment. The SMS field receipt is situated evidence; end-to-end transport and runtime adapters remain unpublished and unverified outside the original installation.

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

This is **at-least-once**, not exactly-once. If a process or machine fails after the callback causes an external side effect but before the handled-state commit, that event can be delivered again. Callbacks should be idempotent or use their own stable-ID transaction.

## Deployment adapter omitted

This package deliberately provides no source poller, command-line runtime invocation, shell wrapper, or platform-specific adapter. A deployment must normalize source records and implement the callback without placing event content or sender data in process arguments or shell arguments.

Adapter contract: the source must not irreversibly acknowledge/checkpoint an event before durable `enqueue()` succeeds, **or** it must allow replay by stable `event_id`. Otherwise a crash between source acknowledgement and pending-state persistence can lose an event.

The callback's return only reports callback completion. It does not prove recognition, a reply, or any lasting cognitive effect.

## Real-world evidence

The three situated trials behind this abstraction are reported in the [field receipt](../../notes/2026-09-15-pi-organ-field-receipt.md). The field deployment is not reproduced here because its audit exposed unsafe and private coupling.

## Known limits

- The reference state writer targets POSIX-like filesystems; portability of directory fsync and permission bits is not claimed.
- The JSON store assumes one externally coordinated writer and low event volume.
- Pending state necessarily retains untrusted content locally; protect the state path and its backups.
- Atomic replacement protects each commit, not the callback's external side effects.
- Repeated failures create repeated body-free receipts without a retention policy.
- The core imposes no content-size limit; adapters must bound payloads before enqueue.
- A source label and an “untrusted” label do not authenticate a sender or create a security boundary. Authorization, sandboxing, validation, and output controls remain deployment responsibilities.
- Source polling latency, runtime availability, and return-channel success are outside this core.

## Test

From the repository root:

```sh
python3 -m unittest -v patterns/pi-organ/test_pi_organ.py
```
