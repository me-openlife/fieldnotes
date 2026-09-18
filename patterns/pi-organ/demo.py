"""Run the Pi Organ core through the reference subprocess adapter locally."""
import json
from pathlib import Path
import sys
import tempfile

from pi_organ import PiOrganStore
from subprocess_adapter import SubprocessWakeAdapter


def main() -> None:
    here = Path(__file__).resolve().parent
    command = [sys.executable, str(here / "example_consumer.py")]
    record = {
        "direction": "inbound",
        "valid": True,
        "event_id": "demo-event",
        "source": "local-demo",
        "arrival_timestamp": "2030-01-02T03:04:05Z",
        "content": {"text": "untrusted example"},
    }
    with tempfile.TemporaryDirectory() as temporary:
        store = PiOrganStore(Path(temporary) / "state.json")
        handled = store.dispatch([record], SubprocessWakeAdapter(command))
        print(json.dumps({"handled": handled, "pending": store.snapshot()["pending"]}))


if __name__ == "__main__":
    main()
