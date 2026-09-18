import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from pi_organ import PiOrganStore
from subprocess_adapter import (
    ConsumerExitError,
    ConsumerTimeoutError,
    PayloadTooLargeError,
    SubprocessWakeAdapter,
)


class SubprocessWakeAdapterTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.directory = Path(self.tempdir.name)
        self.store = PiOrganStore(self.directory / "state.json")
        self.example_consumer = Path(__file__).parent / "example_consumer.py"

    def tearDown(self):
        self.tempdir.cleanup()

    @staticmethod
    def inbound(content="untrusted example"):
        return {
            "direction": "inbound",
            "valid": True,
            "event_id": "adapter-event",
            "source": "local-test",
            "arrival_timestamp": "2030-01-02T03:04:05Z",
            "content": content,
        }

    def write_script(self, name, body):
        path = self.directory / name
        path.write_text(body, encoding="utf-8")
        return path

    def success_adapter(self):
        return SubprocessWakeAdapter([sys.executable, str(self.example_consumer)])

    def test_event_is_delivered_on_stdin_and_not_in_argv(self):
        script = self.write_script(
            "record_input.py",
            """import json
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(json.dumps(sys.argv[1:]), encoding="utf-8")
Path(sys.argv[2]).write_bytes(sys.stdin.buffer.read())
""",
        )
        argv_record = self.directory / "argv.json"
        stdin_record = self.directory / "stdin.json"
        sentinel = "stdin-only-sentinel"
        adapter = SubprocessWakeAdapter(
            [sys.executable, str(script), str(argv_record), str(stdin_record)]
        )

        self.store.dispatch([self.inbound({"text": sentinel})], adapter)

        delivered = json.loads(stdin_record.read_text(encoding="utf-8"))
        self.assertEqual(delivered["content"], {"text": sentinel})
        self.assertEqual(
            set(delivered), {"event_id", "source", "arrival_timestamp", "content"}
        )
        recorded_argv = argv_record.read_text(encoding="utf-8")
        for event_value in (
            sentinel,
            delivered["event_id"],
            delivered["source"],
            delivered["arrival_timestamp"],
        ):
            self.assertNotIn(event_value, recorded_argv)
        self.assertNotIn("content", recorded_argv)

    def test_timeout_stays_pending_and_can_be_retried_through_core(self):
        script = self.write_script(
            "slow.py", "import time\ntime.sleep(1)\n"
        )
        adapter = SubprocessWakeAdapter(
            [sys.executable, str(script)], timeout_seconds=0.05
        )

        with self.assertRaises(ConsumerTimeoutError):
            self.store.dispatch([self.inbound()], adapter)
        self.assertEqual(
            [item["event_id"] for item in self.store.snapshot()["pending"]],
            ["adapter-event"],
        )

        self.assertEqual(
            self.store.process_pending(self.success_adapter()), ["adapter-event"]
        )

    def test_nonzero_exit_stays_pending_and_can_be_retried_through_core(self):
        script = self.write_script("fail.py", "raise SystemExit(7)\n")

        with self.assertRaises(ConsumerExitError) as caught:
            self.store.dispatch(
                [self.inbound()], SubprocessWakeAdapter([sys.executable, str(script)])
            )
        self.assertEqual(caught.exception.returncode, 7)
        self.assertEqual(
            [item["event_id"] for item in self.store.snapshot()["pending"]],
            ["adapter-event"],
        )

        self.assertEqual(
            self.store.process_pending(self.success_adapter()), ["adapter-event"]
        )

    def test_oversize_payload_is_rejected_before_child_start(self):
        marker = self.directory / "started"
        script = self.write_script(
            "mark_started.py",
            "from pathlib import Path\nimport sys\nPath(sys.argv[1]).touch()\n",
        )
        adapter = SubprocessWakeAdapter(
            [sys.executable, str(script), str(marker)], max_payload_bytes=256
        )

        with self.assertRaises(PayloadTooLargeError):
            self.store.dispatch([self.inbound("x" * 512)], adapter)

        self.assertFalse(marker.exists())
        self.assertEqual(
            [item["event_id"] for item in self.store.snapshot()["pending"]],
            ["adapter-event"],
        )

    def test_child_output_is_discarded_and_not_retained_on_failure(self):
        script = self.write_script(
            "emit_and_fail.py",
            """import sys
print("output-retention-sentinel")
print("output-retention-sentinel", file=sys.stderr)
raise SystemExit(9)
""",
        )

        with self.assertRaises(ConsumerExitError) as caught:
            self.store.dispatch(
                [self.inbound()], SubprocessWakeAdapter([sys.executable, str(script)])
            )

        error = caught.exception
        self.assertNotIn("output-retention-sentinel", str(error))
        self.assertFalse(hasattr(error, "stdout"))
        self.assertFalse(hasattr(error, "stderr"))
        receipt = self.store.snapshot()["failures"][0]
        self.assertNotIn("output-retention-sentinel", json.dumps(receipt))
        self.assertEqual(receipt["error_type"], "ConsumerExitError")

    def test_parent_environment_is_not_inherited_without_explicit_opt_in(self):
        script = self.write_script(
            "record_environment.py",
            """import os
from pathlib import Path
import sys
Path(sys.argv[1]).write_text(os.environ.get("PI_ORGAN_TEST_SECRET", "absent"), encoding="utf-8")
""",
        )
        output = self.directory / "environment.txt"
        previous = os.environ.get("PI_ORGAN_TEST_SECRET")
        os.environ["PI_ORGAN_TEST_SECRET"] = "parent-only-sentinel"
        try:
            self.store.dispatch(
                [self.inbound()],
                SubprocessWakeAdapter([sys.executable, str(script), str(output)]),
            )
        finally:
            if previous is None:
                os.environ.pop("PI_ORGAN_TEST_SECRET", None)
            else:
                os.environ["PI_ORGAN_TEST_SECRET"] = previous

        self.assertEqual(output.read_text(encoding="utf-8"), "absent")

    def test_command_string_is_rejected(self):
        with self.assertRaises(TypeError):
            SubprocessWakeAdapter("python example_consumer.py")


if __name__ == "__main__":
    unittest.main()
