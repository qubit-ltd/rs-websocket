# WS-R2 review resolution

## Outcome

- **WS-R2-001 — investigated; candidate B retained.** The synthetic notified-prefix probe met the design's performance gate in all three alternating release comparisons. It does not establish a production latency guarantee or the frequency of this arrangement in deployed sessions. The exact measurements, controls, raw logs, and limits are in [`evidence/review-ea19bd63/decision.md`](evidence/review-ea19bd63/decision.md).
- **WS-R2-002 — fixed.** `WsSessionLimits.outbound_messages` and `outbound_bytes` Rustdoc now state that admitted messages keep their count/logical-payload reservations while queued or being written. The descriptions retain the non-zero and write-buffer-overhead requirements. They match `send_envelope`, `Reservation::drop`, and both user guides.
- No public API or configuration changed. No caller migration is required.

## Changes

- In the indexed queue path with positive free bytes, the selector now scans at most eight actual waiter entries, including notified entries, and then uses the existing byte-bucket fallback if the prefix has no eligible waiter. The fallback still chooses the lowest eligible waiter ID. The small-set and zero-free-byte paths, cancellation/reset handoff, close broadcast, and index hysteresis remain intact.
- Added a K=129 batch-release regression that checks distinct one-time wakes, successful repoll of 64 selected senders, full cleanup, and zero remaining usage. Added private selector tests for a notified prefix, multiple fitting IDs after an unfit waiter, and zero-byte waiters when no byte capacity is free.
- Corrected the two outbound budget field descriptions without changing signatures.
- Independent read-only review found no Critical, Important, or Minor findings.

## Performance decision and limits

For K=1024, candidate batch-release p50 improved 78.7–78.8% across three paired runs; for K=4096, it improved 96.2–96.3%. Payload-diversity, unfit-prefix, threshold-churn, and single-release controls were noisy and had mixed directions; none showed a repeatable regression above 10%. The two-worker queue contention supplement also had substantial scheduling noise. A separate real `WsSession` writer-release test passed on the current-thread runtime. These probes demonstrate a reachable synthetic selector cost and preserve semantic progress; they are not production SLO measurements.

## Validation and evidence

The required default/all-feature tests, strict Rustdoc, coverage, CI, and current-source WS/WSS interoperability all passed. Coverage remained above the repository thresholds without adjustment. Exact commands, exit codes, the initial style failure and fix, and key results are recorded in [`evidence/review-ea19bd63/validation-results.md`](evidence/review-ea19bd63/validation-results.md); full command outputs and current-source metadata are in that evidence directory.

## Workspace and downstream state

- Target checkout at start: `dev-starfish`, HEAD `1715fade68f2f7f8910ccfa5467c55ba908c5963`, with six pre-existing untracked design/plan documents. The two documents for this task remained outside all commits and are preserved in the original checkout.
- Existing unrelated websocket worktree `/data/working/qubit/.worktrees/rs-websocket-ws-r2-001` was left untouched.
- `rs-web` was initially observed on `dev-starfish` at `09952ccbebec0847941877853f064e786d31db09` with two untracked design/plan documents. Before interoperability ran, it was observed at `08e25903894268629e4a979d60d3dae816092801` with a clean status and those two files absent. This task issued no command to change or remove files in that checkout; the interoperability script confirms both input trees did not change during its run. Their intervening disappearance is recorded here without attempting to reconstruct unknown content.
- The current-source check used `rs-web` at `08e2590` and the implementation worktree at `f0a461d`; the JSON records both revisions, hashes, and the unchanged-input result.

## Commits

- `4db14ab docs(websocket): clarify outbound session budgets`
- `f0a461d perf(rs-websocket): bound indexed waiter prefix scans`
- `33bb125 style(session): align selector test names`

The design and implementation-plan documents remain on disk in the original project checkout and were not copied into or included in the implementation commits.
