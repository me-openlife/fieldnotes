import gc
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
import weakref

sys.path.insert(0, str(Path(__file__).parent))
from pi_organ import InboundEvent, PiOrganStore, SourceIsolationError, StateError


class PiOrganTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.tempdir.name) / "state.json"
        self.store = PiOrganStore(self.state_path)

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def inbound(
        event_id="example-event-1",
        content="example payload",
        source="sensor.example.invalid",
    ):
        return {
            "direction": "inbound",
            "valid": True,
            "event_id": event_id,
            "source": source,
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

    def test_event_id_deduplication_is_global_across_sources(self):
        calls = []
        self.store.dispatch(
            [
                self.inbound("provider-id", content="first", source="source-a"),
                self.inbound("provider-id", content="collision", source="source-b"),
            ],
            calls.append,
        )

        self.assertEqual(
            [(event.source, event.content) for event in calls],
            [("source-a", "first")],
        )
        self.assertEqual(self.store.snapshot()["handled"], ["provider-id"])

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

    def test_default_failure_policy_remains_fail_fast(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
        ]
        calls = []
        original = RuntimeError("original callback failure")

        def wake(event):
            calls.append(event.event_id)
            raise original

        with self.assertRaises(RuntimeError) as caught:
            self.store.dispatch(records, wake)

        self.assertIs(caught.exception, original)
        self.assertEqual(calls, ["a-1"])
        self.assertEqual(
            [item["event_id"] for item in self.store.snapshot()["pending"]],
            ["a-1", "b-1"],
        )

    def test_successful_callback_receives_detached_nested_content(self):
        original = {"nested": {"items": ["original"]}}
        durable_during_callback = []

        def mutate_then_succeed(event):
            event.content["nested"]["items"].append("callback-only")
            durable_during_callback.append(
                PiOrganStore(self.state_path).snapshot()["pending"][0]["content"]
            )

        self.store.dispatch([self.inbound(content=original)], mutate_then_succeed)

        self.assertEqual(durable_during_callback, [original])
        self.assertEqual(original, {"nested": {"items": ["original"]}})
        self.assertEqual(self.store.snapshot()["pending"], [])

    def test_failed_callback_nested_mutation_does_not_change_retry_content(self):
        original = {"nested": {"items": [{"value": "original"}]}}

        def mutate_then_fail(event):
            event.content["nested"]["items"][0]["value"] = "mutated"
            event.content["nested"]["items"].append({"value": "added"})
            raise RuntimeError("temporary")

        with self.assertRaises(RuntimeError):
            self.store.dispatch([self.inbound(content=original)], mutate_then_fail)

        self.assertEqual(self.store.snapshot()["pending"][0]["content"], original)
        retried = []
        self.store.process_pending(retried.append)
        self.assertEqual([event.content for event in retried], [original])

    def test_non_json_callback_mutation_isolated_and_retry_gets_original(self):
        original = {"nested": {"items": ["original"]}}
        records = [
            self.inbound("failed", content=original, source="source-a"),
            self.inbound("unrelated", content="ok", source="source-b"),
        ]
        calls = []

        def mutate_then_fail(event):
            calls.append(event.event_id)
            if event.event_id == "failed":
                event.content["nested"]["items"].append(object())
                raise ValueError("temporary")

        with self.assertRaises(SourceIsolationError) as caught:
            self.store.dispatch(
                records, mutate_then_fail, failure_policy="isolate_sources"
            )

        self.assertEqual(caught.exception.failure_count, 1)
        self.assertEqual(calls, ["failed", "unrelated"])
        state = self.store.snapshot()
        self.assertEqual(state["pending"][0]["content"], original)
        self.assertEqual(state["handled"], ["unrelated"])
        self.assertEqual(
            state["failures"],
            [
                {
                    "source": "source-a",
                    "event_id_hash": hashlib.sha256(b"failed").hexdigest(),
                    "error_type": "ValueError",
                }
            ],
        )

        retried = []
        PiOrganStore(self.state_path).process_pending(retried.append)
        self.assertEqual([event.content for event in retried], [original])

    def test_source_isolation_keeps_failed_source_order_and_handles_other_source(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
            self.inbound("a-2", source="source-a"),
        ]
        calls = []

        def wake(event):
            calls.append(event.event_id)
            if event.event_id == "a-1":
                raise RuntimeError("source-a unavailable")

        with self.assertRaises(SourceIsolationError) as caught:
            self.store.dispatch(records, wake, failure_policy="isolate_sources")

        self.assertEqual(caught.exception.failure_count, 1)
        self.assertEqual(caught.exception.failed_sources, ("source-a",))
        self.assertEqual(calls, ["a-1", "b-1"])
        state = self.store.snapshot()
        self.assertEqual([item["event_id"] for item in state["pending"]], ["a-1", "a-2"])
        self.assertIn("b-1", state["handled"])

    def test_source_isolation_retry_processes_failed_source_in_order(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
            self.inbound("a-2", source="source-a"),
        ]

        def fail_a1(event):
            if event.event_id == "a-1":
                raise RuntimeError("temporarily unavailable")

        with self.assertRaises(SourceIsolationError):
            self.store.dispatch(records, fail_a1, failure_policy="isolate_sources")

        retried = []
        completed = self.store.process_pending(retried.append)
        self.assertEqual(completed, ["a-1", "a-2"])
        self.assertEqual([event.event_id for event in retried], ["a-1", "a-2"])
        self.assertEqual(self.store.snapshot()["pending"], [])

    def test_source_isolation_blocks_each_failed_source_only(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
            self.inbound("a-2", source="source-a"),
            self.inbound("c-1", source="source-c"),
            self.inbound("b-2", source="source-b"),
        ]
        calls = []

        def wake(event):
            calls.append(event.event_id)
            if event.event_id in {"a-1", "b-1"}:
                raise LookupError("unavailable")

        with self.assertRaises(SourceIsolationError) as caught:
            self.store.dispatch(records, wake, failure_policy="isolate_sources")

        self.assertEqual(caught.exception.failure_count, 2)
        self.assertEqual(caught.exception.failed_sources, ("source-a", "source-b"))
        self.assertEqual(calls, ["a-1", "b-1", "c-1"])
        state = self.store.snapshot()
        self.assertEqual(
            [item["event_id"] for item in state["pending"]],
            ["a-1", "b-1", "a-2", "b-2"],
        )
        self.assertIn("c-1", state["handled"])
        self.assertEqual(len(state["failures"]), 2)

    def test_source_isolation_commits_successes_before_and_after_failure(self):
        records = [
            self.inbound("before", source="source-b"),
            self.inbound("failed", source="source-a"),
            self.inbound("after", source="source-c"),
        ]
        calls = []

        def wake(event):
            calls.append(event.event_id)
            if event.event_id == "failed":
                raise RuntimeError("unavailable")

        with self.assertRaises(SourceIsolationError):
            self.store.dispatch(records, wake, failure_policy="isolate_sources")

        self.assertEqual(calls, ["before", "failed", "after"])
        state = self.store.snapshot()
        self.assertEqual(state["handled"], ["after", "before"])
        self.assertEqual([item["event_id"] for item in state["pending"]], ["failed"])

        completed = self.store.process_pending(lambda _event: None)
        self.assertEqual(completed, ["failed"])

    def test_later_callback_observes_prior_success_and_failure_receipt_committed(self):
        records = [
            self.inbound("success", source="source-success"),
            self.inbound("failure", source="source-failure"),
            self.inbound("observer", source="source-observer"),
        ]
        observer_snapshots = []

        def wake(event):
            if event.event_id == "failure":
                raise LookupError("temporary")
            if event.event_id == "observer":
                observer_snapshots.append(PiOrganStore(self.state_path).snapshot())

        with self.assertRaises(SourceIsolationError):
            self.store.dispatch(records, wake, failure_policy="isolate_sources")

        self.assertEqual(len(observer_snapshots), 1)
        observed = observer_snapshots[0]
        self.assertIn("success", observed["handled"])
        self.assertNotIn("observer", observed["handled"])
        self.assertEqual(
            [item["event_id"] for item in observed["pending"]],
            ["failure", "observer"],
        )
        self.assertEqual(len(observed["failures"]), 1)
        self.assertEqual(observed["failures"][0]["error_type"], "LookupError")

    def test_isolation_policy_returns_all_ids_when_the_pass_has_no_failures(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
        ]
        self.assertEqual(
            self.store.dispatch(
                records, lambda _event: None, failure_policy="isolate_sources"
            ),
            ["a-1", "b-1"],
        )

    def test_invalid_failure_policy_precedes_callback_and_state_mutation(self):
        calls = []
        records_iterated = []

        def records():
            records_iterated.append(True)
            yield self.inbound()

        with self.assertRaises(ValueError):
            self.store.dispatch(records(), calls.append, failure_policy="keep_going")

        self.assertEqual(calls, [])
        self.assertEqual(records_iterated, [])
        self.assertFalse(self.state_path.exists())

        self.store.enqueue([self.inbound()])
        before = self.store.snapshot()
        with self.assertRaises(ValueError):
            self.store.process_pending(calls.append, failure_policy="unknown")
        self.assertEqual(calls, [])
        self.assertEqual(self.store.snapshot(), before)

    def test_isolation_summary_and_receipt_are_body_free(self):
        raw_event_id = "raw-private-event-id"
        private_body = "private-body-sentinel"
        private_sender = "sender-private@example.invalid"
        original_message = "original-exception-message-sentinel"
        record = self.inbound(
            raw_event_id,
            content={"body": private_body, "sender": private_sender},
            source="sms",
        )
        callback_error_refs = []

        class CallbackError(ValueError):
            pass

        def fail(_event):
            error = CallbackError(original_message)
            callback_error_refs.append(weakref.ref(error))
            raise error

        with self.assertRaises(SourceIsolationError) as caught:
            self.store.dispatch([record], fail, failure_policy="isolate_sources")

        summary = caught.exception
        exposed_summary = json.dumps(
            {
                "text": str(summary),
                "repr": repr(summary),
                "args": summary.args,
                "attributes": summary.__dict__,
            }
        )
        receipt = self.store.snapshot()["failures"][0]
        exposed_receipt = json.dumps(receipt)
        for sensitive in (
            raw_event_id,
            private_body,
            private_sender,
            original_message,
        ):
            self.assertNotIn(sensitive, exposed_summary)
            self.assertNotIn(sensitive, exposed_receipt)
        self.assertEqual(summary.failure_count, 1)
        self.assertEqual(summary.failed_sources, ("sms",))
        self.assertIsNone(summary.__cause__)
        self.assertIsNone(summary.__context__)
        gc.collect()
        self.assertEqual(len(callback_error_refs), 1)
        self.assertIsNone(callback_error_refs[0]())
        self.assertEqual(
            receipt["event_id_hash"], hashlib.sha256(raw_event_id.encode()).hexdigest()
        )

    def test_isolation_summary_traceback_omits_sensitive_processing_locals(self):
        raw_event_id = "trace-private-event-id"
        private_body = "trace-private-body"

        def fail(_event):
            raise RuntimeError("callback failure")

        try:
            self.store.dispatch(
                [self.inbound(raw_event_id, content={"body": private_body}, source="sms")],
                fail,
                failure_policy="isolate_sources",
            )
        except SourceIsolationError as error:
            traceback_frames = []
            traceback_locals = []
            traceback = error.__traceback__
            while traceback is not None:
                frame = traceback.tb_frame
                if frame.f_globals.get("__name__") == "pi_organ":
                    traceback_frames.append(frame.f_code.co_name)
                    traceback_locals.append(repr(frame.f_locals))
                traceback = traceback.tb_next
        else:
            self.fail("SourceIsolationError was not raised")

        self.assertNotIn("_process_pending_sensitive", traceback_frames)
        exposed_locals = "\n".join(traceback_locals)
        self.assertNotIn(raw_event_id, exposed_locals)
        self.assertNotIn(private_body, exposed_locals)

    def test_restart_preserves_isolated_pending_order_and_retry_semantics(self):
        records = [
            self.inbound("a-1", source="source-a"),
            self.inbound("b-1", source="source-b"),
            self.inbound("a-2", source="source-a"),
        ]

        def wake(event):
            if event.event_id == "a-1":
                raise OSError("temporary")

        with self.assertRaises(SourceIsolationError):
            self.store.dispatch(records, wake, failure_policy="isolate_sources")

        restarted = PiOrganStore(self.state_path)
        state = restarted.snapshot()
        self.assertEqual([item["event_id"] for item in state["pending"]], ["a-1", "a-2"])
        self.assertIn("b-1", state["handled"])

        calls = []
        self.assertEqual(restarted.process_pending(calls.append), ["a-1", "a-2"])
        self.assertEqual([event.event_id for event in calls], ["a-1", "a-2"])

        another_restart = PiOrganStore(self.state_path)
        self.assertEqual(another_restart.process_pending(calls.append), [])

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
