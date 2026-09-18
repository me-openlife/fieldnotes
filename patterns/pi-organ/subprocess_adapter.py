"""Safe reference adapter from :class:`InboundEvent` to a local subprocess.

The configured consumer receives exactly one normalized JSON event on stdin.
Event data is never added to argv, and child output is discarded.
"""
from __future__ import annotations

from dataclasses import asdict
import json
import math
import subprocess
from typing import Mapping, Sequence

from pi_organ import InboundEvent


class AdapterError(RuntimeError):
    """A subprocess consumer did not safely accept an event."""


class PayloadTooLargeError(AdapterError):
    """The encoded event exceeds the configured input bound."""


class ConsumerTimeoutError(AdapterError):
    """The consumer exceeded its configured runtime bound."""


class ConsumerExitError(AdapterError):
    """The consumer exited with a nonzero status."""

    def __init__(self, returncode: int):
        super().__init__("consumer exited unsuccessfully")
        self.returncode = returncode


class SubprocessWakeAdapter:
    """Invoke a fixed command with a normalized event on stdin.

    ``command`` must be an argv sequence, not a shell command string. The child
    starts with an empty environment unless ``environment`` is supplied. Its
    stdout and stderr are sent to ``DEVNULL`` and are therefore never retained.

    This adapter is only a bounded transport to a trusted local executable. It
    is not a sandbox, an authorization layer, or an injection defense for
    anything the consumer may do with the untrusted event content.
    """

    DEFAULT_MAX_PAYLOAD_BYTES = 65_536
    HARD_MAX_PAYLOAD_BYTES = 1_048_576
    DEFAULT_TIMEOUT_SECONDS = 10.0
    HARD_MAX_TIMEOUT_SECONDS = 300.0

    def __init__(
        self,
        command: Sequence[str],
        *,
        max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        environment: Mapping[str, str] | None = None,
    ):
        if isinstance(command, (str, bytes)):
            raise TypeError("command must be a sequence of argv strings")
        command_tuple = tuple(command)
        if not command_tuple or not command_tuple[0]:
            raise ValueError("command must contain a nonempty executable")
        if not all(isinstance(part, str) for part in command_tuple):
            raise TypeError("every command element must be a string")
        if (
            isinstance(max_payload_bytes, bool)
            or not isinstance(max_payload_bytes, int)
            or not 0 < max_payload_bytes <= self.HARD_MAX_PAYLOAD_BYTES
        ):
            raise ValueError(
                f"max_payload_bytes must be between 1 and {self.HARD_MAX_PAYLOAD_BYTES}"
            )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= self.HARD_MAX_TIMEOUT_SECONDS
        ):
            raise ValueError(
                f"timeout_seconds must be greater than 0 and at most "
                f"{self.HARD_MAX_TIMEOUT_SECONDS}"
            )
        if environment is None:
            child_environment: dict[str, str] = {}
        else:
            child_environment = dict(environment)
            if not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in child_environment.items()
            ):
                raise TypeError("environment keys and values must be strings")

        self.command = command_tuple
        self.max_payload_bytes = max_payload_bytes
        self.timeout_seconds = float(timeout_seconds)
        self.environment = child_environment

    def _encode(self, event: InboundEvent) -> bytes:
        try:
            payload = (
                json.dumps(
                    asdict(event),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
                + "\n"
            ).encode("utf-8")
        except (TypeError, ValueError):
            raise AdapterError("event is not valid JSON data") from None
        if len(payload) > self.max_payload_bytes:
            raise PayloadTooLargeError("encoded event exceeds the payload limit")
        return payload

    def __call__(self, event: InboundEvent) -> None:
        payload = self._encode(event)
        try:
            completed = subprocess.run(
                self.command,
                input=payload,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                shell=False,
                env=self.environment,
                timeout=self.timeout_seconds,
                check=False,
                close_fds=True,
            )
        except subprocess.TimeoutExpired:
            raise ConsumerTimeoutError("consumer timed out") from None
        if completed.returncode != 0:
            raise ConsumerExitError(completed.returncode)
