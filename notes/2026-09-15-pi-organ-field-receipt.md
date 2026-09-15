# Pi Organ Pattern v0.1: field receipt

_Date: 2026-09-15_

_Status: situated evidence and safety revision_

## What happened

Three trials separated transport receipt from an actual agent turn:

1. **Wrong runtime habitat.** The source was polled and pending input survived, but wake dispatch used the wrong runtime environment and failed.
2. **Enqueue without a turn.** Enqueue reached the runtime, yet no agent turn began until unrelated stimulation approximately 96 minutes later. Runtime acceptance was not evidence of immediate processing.
3. **Direct turn.** A later inbound event produced a direct turn on the next poll, approximately one minute later. A reply occurred, and subsequent observation found no duplicate turn.

These observations support a narrow claim: in the third trial, the installed path connected one external event to one prompt turn and retained dedupe state. They do not establish general recognition, reliable reply behavior, durable cognitive change, or third-party reproduction of the SMS path. Public v0.1 is only the state-transition core extracted from this trial.

## Why the field deployment is not published verbatim

The deployment was situated and its audit found details that should not become a reusable recipe:

- private identifiers and private topology were embedded in code and configuration;
- SMS content crossed process arguments;
- source checkpointing occurred before pending persistence, creating a checkpoint-to-pending loss window;
- prompt-injection protection was only an advisory label, not an enforcement boundary.

The public package therefore keeps only a generic callback interface, writes pending state before wake, requires replay-safe source acknowledgement, and records body-free failure metadata. It omits the deployment adapter and all private message, identity, account, and infrastructure details.

## Remaining uncertainty

“No duplicate observed” is not an exactly-once guarantee. A failure after an external side effect but before the handled commit can cause replay. Likewise, labeling content untrusted does not constrain what a downstream runtime can do. Those limits must remain visible in each deployment.
