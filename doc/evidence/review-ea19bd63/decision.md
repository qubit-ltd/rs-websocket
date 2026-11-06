# WS-R2-001 evidence and candidate decision

## Decision

**Retain candidate B**: in the large indexed waiter path with positive free bytes, scan at most the first eight actual `state.waiters` entries, selecting the first unnotified fitting waiter; if none fits, use the existing exact byte-bucket fallback. The scan limit counts notified entries too. Small/non-indexed selection, `free_bytes == 0` selection, byte-bucket indexing, hysteresis, reset/cancel handoff, and close broadcast remain unchanged.

The measured synthetic batch-release case exceeded the design's minimum gain by a wide margin in three alternating baseline/candidate groups. Controls were noisy and had mixed signs; none showed a repeatable regression above 10%. The evidence does not establish a production latency guarantee or show that the synthetic arrangement is common in deployed sessions.

## Baseline and method

- T1 started at worktree HEAD `4db14ab7dba30e194f310c6e47886875ceb6676c` (the disjoint T2 Rustdoc commit); the T1 production selector matched reference HEAD `1715fade68f2f7f8910ccfa5467c55ba908c5963`.
- Synthetic batch probe: fill a `K`-message/`K`-byte `BoundedQueue`, manually poll `K` one-byte push futures to `Pending`, then drop `batch` queued envelopes without repolling the waiters. Only reservation release is timed; construction, cleanup and logging are outside the timed region. K = 16, 128, 129, 1024, 4096; batch = 1, min(16,K), K; 3 warmups, 30 samples, 3 rounds. The probe is current-thread and intentionally creates a notified prefix.
- Candidate/baseline are separate release test binaries with SHA-256 in `binary-baseline-sha256.txt`. Three paired groups ran baseline then candidate. Full raw samples are in `paired-*.log`; initial baseline output is retained in `batch-release-baseline.log`.

| K, batch=K | Group | Baseline total p50 / p95 | Candidate total p50 / p95 | p50 change |
| --- | ---: | ---: | ---: | ---: |
| 1024 | 1 | 1,406,993 / 1,537,469 ns | 300,005 / 310,121 ns | −78.7% (−1,106,988 ns) |
| 1024 | 2 | 1,424,532 / 1,511,391 ns | 301,488 / 312,393 ns | −78.8% (−1,123,044 ns) |
| 1024 | 3 | 1,411,387 / 1,490,175 ns | 300,206 / 333,219 ns | −78.7% (−1,111,181 ns) |
| 4096 | 1 | 33,807,270 / 35,366,837 ns | 1,247,223 / 1,334,935 ns | −96.3% (−32,560,047 ns) |
| 4096 | 2 | 33,424,161 / 40,037,978 ns | 1,256,103 / 1,316,179 ns | −96.2% (−32,168,058 ns) |
| 4096 | 3 | 33,866,735 / 34,802,211 ns | 1,240,918 / 1,258,566 ns | −96.3% (−32,625,817 ns) |

The 1024 and 4096 p50 reductions clear the design's 20% gate in all three groups. Candidate K=4096 p50 is about 1.24–1.26 ms for 4096 releases (about 303–307 ns/release); baseline is about 33.4–33.9 ms for the batch. Some runs show scheduler/load outliers, visible in raw samples and p95.

## Control matrix and limits

- Single-release (`batch=1`) cases are included in every paired batch-probe run. Across the three groups, pooled p50 changed by approximately 0%, +2.3%, +4.0%, −4.3%, and −12.9% for K = 16, 128, 129, 1024, and 4096 respectively. The larger case improved; none showed a stable regression.
- Full payload-diversity probe ran in three paired groups (36 K=4096 order/target summaries per binary/group). Median across those summaries was baseline→candidate 1,252.5→1,116 ns, 2,121.5→1,743.5 ns, and 1,746.5→1,189 ns. Per-scenario signs varied substantially across runs; for example, K=4096 descending/quarter candidate deltas were −7%, −22%, and +139%. Raw files `control-payload-*.log` show this timing noise; no scenario demonstrated a stable candidate regression.
- Unfit-prefix probe ran three paired times, seven samples each. K=4096 baseline/candidate release p50s were 2,434/3,528 ns, 1,403/1,170 ns, and 1,225/1,494 ns. K=16384 values were 4,633/5,630 ns, 2,830/2,530 ns, and 3,454/1,670 ns. Signs reverse between pairs. Pooled p50 was 1,403→1,494 ns (+6.5%) at K=4096 and 3,454→2,530 ns (−26.7%) at K=16384. This does not show a stable >10% regression; absolute shifts are small relative to the observed run-to-run drift. Raw seven-sample measurements are in `control-unfit-*.log`.
- Threshold-churn probe ran baseline and candidate once; cycle p50s changed by roughly −3.4% to +4.8% across K=16..1024, with no stable regression. It also exercised its two-worker contention subcase. Raw samples are in `control-churn-*.log`.
- The runtime scheduling supplement `runtime-two-worker-*.log` is a Tokio multi-thread (2 worker) `BoundedQueue` contention probe with spawned waiter and competitor tasks, not a WebSocket session-driver probe. It observed competition and backpressure (both binaries: 21 competitor attempts, 11 accepted, 10 backpressure); release latency had broad ranges (baseline 1.12–17.7 μs, candidate 1.15–22.7 μs) and does not support a latency comparison.
- Separately, `real-session-driver-test.log` records an actual `WsSession` sender waiting until the writer releases an outbound permit, then succeeding. It ran on Tokio's default current-thread test runtime. This confirms the existing real driver progress path, not the notified-prefix batch pattern or a two-worker session schedule.

## Semantic and build evidence

- Added `test_batch_release_selects_distinct_waiters_before_repoll`: K=129; after each of 64 releases, exactly the next waiter has one wake; the first 64 then admit successfully, all futures/envelopes are cleaned up, and usage is `(0,0)`.
- Added private selector tests: after eight notified entries and an unfit entry, fallback selects the minimum ID among multiple fitting waiters; with zero free bytes a zero-byte waiter remains selectable.
- `cargo test --locked --lib session::`: 107 passed, 11 ignored, 35 filtered.
- `cargo test --locked --test session_tests test_send_waiting_on_capacity_succeeds_after_writer_releases_a_permit`: 1 passed, 43 filtered.
- `commands.txt` records the initial missing local-path dependency failure, setup, a test-only missing-import compile failure and fix, the zsh wrapper exit artifact, and all successful focused/probe commands.

Environment, runtime notes, baseline/candidate source hashes, test binary hashes, and per-command logs are stored in this directory. The probe's controlled manual-poll arrangement proves the selector can see a long notified prefix; the real session test and two-worker queue supplement are separate, narrower observations.
