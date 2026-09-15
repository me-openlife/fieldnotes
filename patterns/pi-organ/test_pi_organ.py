import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from pi_organ import InboundEvent, PiOrganStore, StateError


class PiOrganTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tempdir.name) / "state.json"
        self.store = PiOrganStore(self.state_path)

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def inbound(event_id="example-event-1", content="example payload"):
        return {
            "direction": "inbound",
            "valid": True,
            "event_id": event_id,
            "source": "sensor.example.invalid",
            "arrival_timestamp": "2030-01-02T03:04:05Z",
            "content": content,
        }

    def test_baseline_does_not_replay_history(self):
        old = self.inbound("example-history")
        self.store.baseline([old])
        calls = []
        self.assertEqual(self.store.dispatch([old], calls.append), [])
        self.assertEqual(calls, [])

    def test_new_inbound_wakes_once_and_is_enqueued_first(self):
        seen = []
        def wake(event):
            self.assertEqual(self.store.snapshot()["pending"][0]["event_id"], event.event_id)
            seen.append(event)
        record = self.inbound()
        self.store.dispatch([record], wake)
        self.store.dispatch([record], wake)
        self.assertEqual(seen, [InboundEvent(record["event_id"], record["source"],
                                             record["arrival_timestamp"], record["content"])])

    def test_outbound_and_invalid_records_are_ignored(self):
        outbound = {**self.inbound("example-outbound"), "direction": "outbound"}
        invalid = {**self.inbound("example-invalid"), "valid": False}
        malformed = {"direction": "inbound", "valid": True, "event_id": "example-short"}
        calls = []
        self.store.dispatch([outbound, invalid, malformed, object()], calls.append)
        self.assertEqual(calls, [])
        self.assertEqual(self.store.snapshot()["pending"], [])

    def test_wake_failure_stays_pending_and_retries(self):
        def fail(_event):
            raise RuntimeError("example failure")
        with self.assertRaises(RuntimeError):
            self.store.dispatch([self.inbound()], fail)
        self.assertEqual([x["event_id"] for x in self.store.snapshot()["pending"]],
                         ["example-event-1"])
        calls = []
        self.assertEqual(self.store.process_pending(calls.append), ["example-event-1"])
        self.assertEqual(len(calls), 1)

    def test_failure_receipt_excludes_content_and_sender(self):
        private_body = "example private body"
        sender = "sender@example.invalid"
        record = self.inbound(content={"body": private_body, "sender": sender})
        def fail(_event):
            raise ValueError("do not persist exception messages")
        with self.assertRaises(ValueError):
            self.store.dispatch([record], fail)
        receipt = self.store.snapshot()["failures"][0]
        self.assertEqual(receipt, {
            "source": "sensor.example.invalid",
            "event_id_hash": hashlib.sha256(b"example-event-1").hexdigest(),
            "error_type": "ValueError",
        })
        encoded = json.dumps(receipt)
        self.assertNotIn(private_body, encoded)
        self.assertNotIn(sender, encoded)
        self.assertNotIn("body", receipt)
        self.assertNotIn("sender", receipt)

    def test_inspection_has_no_side_effect(self):
        candidate = self.store.inspect_new([self.inbound()])
        self.assertEqual([x.event_id for x in candidate], ["example-event-1"])
        self.assertFalse(self.state_path.exists())

    def test_dedupe_persists_across_store_instances(self):
        self.store.dispatch([self.inbound()], lambda _event: None)
        replacement = PiOrganStore(self.state_path)
        calls = []
        replacement.dispatch([self.inbound()], calls.append)
        self.assertEqual(calls, [])

    def test_partial_batch_failure_preserves_progress_and_tail(self):
        records = [self.inbound(f"example-event-{n}") for n in range(1, 4)]
        calls = []
        def wake(event):
            calls.append(event.event_id)
            if event.event_id == "example-event-2":
                raise RuntimeError("example stop")
        with self.assertRaises(RuntimeError):
            self.store.dispatch(records, wake)
        state = self.store.snapshot()
        self.assertIn("example-event-1", state["handled"])
        self.assertEqual([x["event_id"] for x in state["pending"]],
                         ["example-event-2", "example-event-3"])
        retried = []
        PiOrganStore(self.state_path).process_pending(retried.append)
        self.assertEqual([x.event_id for x in retried],
                         ["example-event-2", "example-event-3"])

    def test_malformed_state_fails_closed_without_wake(self):
        self.state_path.write_text('{"version":1,"pending":"not-a-list"}', encoding="utf-8")
        calls = []
        with self.assertRaises(StateError):
            self.store.dispatch([self.inbound()], calls.append)
        self.assertEqual(calls, [])

    @unittest.skipUnless(os.name == "posix", "POSIX permission bits required")
    def test_state_file_mode_is_private(self):
        self.store.baseline([])
        mode = stat.S_IMODE(self.state_path.stat().st_mode)
        self.assertEqual(mode, 0o600)


if __name__ == "__main__":
    unittest.main()
