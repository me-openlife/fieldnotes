"""Minimal consumer for the Pi Organ subprocess adapter example.

It validates one bounded JSON event from stdin, performs no side effect, emits
no event data, and uses its exit status as the callback result.
"""
import json
import sys

MAX_INPUT_BYTES = 65_536
EXPECTED_KEYS = {"event_id", "source", "arrival_timestamp", "content"}


def main() -> int:
    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        return 2
    try:
        event = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return 2
    if not isinstance(event, dict) or set(event) != EXPECTED_KEYS:
        return 2
    if not all(
        isinstance(event.get(key), str) and event[key]
        for key in ("event_id", "source", "arrival_timestamp")
    ):
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
