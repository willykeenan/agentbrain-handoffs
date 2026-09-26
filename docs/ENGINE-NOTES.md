# Engine notes

Fixes made so Part A of `docs/SPEC.md` and the engine docstring hold. Each item is a real defect, not a test relaxation.

## `engine.py`

1. **Enrollment skipped dead inboxes.** `enroll_new` selected unread handoffs in created order and treated a missing recipient as a soft skip. Mail for a removed agent still occupied the `LIMIT`, so newer live mail was never enrolled. The query now joins `agents` and still honours `enabledAfter` (coerced to a float so a JSON `null` cannot drop the filter).

2. **Duplicate rows hid the reason and blocked later copies.** `enroll` wrote the duplicate explanation only to `events`, leaving `handoffs.detail` empty. The match also treated terminal `DUPLICATE` rows as still in progress, so an identical message after the original had finished was marked `DUPLICATE` again. Detail is stored on the row, and only `PENDING` + `ACTIVE` rows count as in-flight duplicates.

3. **A body edit after the provider write failed the delivered turn.** `step` hashed the inbox body before reconciling `ACTIVE` rows, so a later edit of the original message reported `FAILED` for a turn that had already been accepted. Active rows now reconcile first; the hash check still fails pending deliveries that changed before any write.

4. **A crashed send never left `SENDING`.** Idle `SENDING` reconciliation called `update()`, which refreshed `updated` every tick, so `clock - updated` never passed `SENDING_TIMEOUT`. Reconciliation now leaves `updated` at reservation time until the timeout, an observation, or a real state change. Idle checks of other active rows keep `next_check` at 10 s (not 30 s) so a `RETURNED` observation is applied on the next pass and a later identical message is no longer treated as a duplicate of a finished delivery.

5. **Adapter errors after a reserved write became `UNAVAILABLE`.** `tick` classified the exception from the in-memory row loaded *before* `reserve()`, which was still `WAITING` with `attempts=0`. It now re-reads the persisted row: a reserved `ACTIVE` attempt with `attempts >= 1` is `UNCERTAIN` (never resent); a failure before the write stays `UNAVAILABLE`.

6. **A blocked deadline alert aborted the tick.** `alert_overdue` called `Store.send` for `service:deadline`. An explicit block makes `allowed()` false, `send` raises, and later handoffs in that tick never ran. Send failures are ignored, `overdue_alerted` is still set, and the rest of the tick continues. The notification stays non-waking (`intent='notification'`).

7. **A second refusal wedged the recipient.** After `OWNER_REJECTED` and its one retry, a second `NotAccepted` left the row `UNCERTAIN`. Nothing could observe a turn that the app said it never took, so the row stayed in flight and every later handoff to that agent waited `BUSY` forever. Two explicit refusals prove non-delivery, so the row now ends `FAILED` with `resendDecision: final` (flagged for attention, never resent).

8. **Accepted turns nobody could observe stayed in flight forever.** An `ACCEPTED`/`RUNNING` row whose observation kept returning no status (for example after the process that watched a command run restarted) was rewritten unchanged every pass. It now becomes `UNCERTAIN` after 10 minutes without an observation (`unobservedSince` in the receipt, reset by any real observation), and the proven-absent rule applies to it from there.

9. **Queued rows starved other recipients.** In `reserve()`, a row waiting behind an in-flight delivery only moved `next_check` when its status or detail changed, so an unchanged `BUSY` row was due on every pass. Sixteen of them filled the `LIMIT 16` scan (ordered by creation) and no other recipient was ever started. Unchanged waiting rows are now re-checked 10 s later, and the scan is ordered by `next_check, created`.

10. **Duplicate suppression dropped distinct work.** Two work assignments with the same text, or two returns with the same summary, have identical bodies, so the second was enrolled as `DUPLICATE` and never delivered. Only plain messages are deduplicated now, and only against other plain messages.

11. **Spawned turns acted as the wrong agent.** The card's `handoffs return` and `handoffs close` lines had no `--as`, and turns inherited the engine's `HANDOFFS_AGENT`. The lines now carry `--as <recipient>`, and the command, Claude Code and Codex adapters set `HANDOFFS_AGENT` (and `HANDOFFS_DB` when known) for the turn.

12. **Short turns never showed as Working.** `ACCEPTED` was re-checked after 10 s, longer than most demo turns. A turn accepted in the last 2 minutes is now checked every 2 s, older ones every 10 s.

13. **A one-shot tick started turns it could not host.** Codex and Claude Code turns live inside the process that starts them; `handoffs tick` exited right after starting one. `Engine(one_shot=True)` (used by `tick`) leaves rows for adapters marked `needs_host` `WAITING` for a long-running engine, and `tick` closes its adapters on exit.

Items 7–13 came from an independent review; each has a regression test in `tests/`.

## `store.py`

`Store.release(id, note)` lets an operator cancel one unfinished delivery (`handoffs release`). It never sends or resends anything. No other store behaviour was changed. `allowed()` already lets `service:` senders through unless an explicit block row exists; that is the contract `send` and the deadline path rely on. Work methods already restrict return/accept to the recipient and close to the sender.

## `tests/test_engine.py`

14. **`test_no_resend_once_the_work_is_returned_closed_or_read` called the wrong agent.** Each case is delivered to a fresh recipient from `add_agents` so the cases do not contend for one in-flight slot, but return/read were hard-coded as `engineer`. That contradicts the store permission rules (and SPEC: only the recipient returns, only the sender closes). Return and read now use the actual recipient; close still uses the original sender.

## `tests/test_store.py`

New. Covers construction, agent ids, settings, directed connections (including `service:` and explicit mode), send validation, idempotent keys, inbox/read, and work lifecycle permissions.
