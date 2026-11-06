# T3 validation record

Project commands were run in `/data/working/qubit/.worktrees/rs-websocket/ws-r2-20261106-ea19bd63` on Rust 1.94.0. Full test, CI, coverage, and interoperability output is stored alongside this file. T1's focused/performance command record is in [`commands.txt`](commands.txt), and its raw performance evidence and decision are in [`decision.md`](decision.md).

| Command | Exit | Result / failure summary |
| --- | ---: | --- |
| `./.infra/bin/align-ci.sh` (first run) | 1 | Repository style rejected the two new private test names because they lacked the required behavior-oriented `test_*` prefix. Renamed both tests to `test_indexed_fallback_*`; no behavior changed. |
| `./.infra/bin/align-ci.sh` (after fix) | 0 | Formatting/style alignment and its CI check passed. Only the two already-modified Rust files were formatted. |
| `./.infra/bin/style-check.sh` | 0 | Rust mechanical style checks passed. |
| `cargo check --workspace --all-targets` | 0 | All workspace targets checked successfully. |
| `cargo test --locked` | 0 | Library: 141 passed, 12 ignored; integration suites and 16 doctests passed. Full output: [`final-cargo-test-locked.txt`](final-cargo-test-locked.txt). |
| `cargo test --all-features --locked` | 0 | Library: 143 passed, 12 ignored; integration suites and 16 doctests passed. Full output: [`final-cargo-test-all-features-locked.txt`](final-cargo-test-all-features-locked.txt). |
| `cargo test --all-features --locked --doc` | 0 | 16 doctests passed. Full output: [`final-cargo-test-all-features-doc-locked.txt`](final-cargo-test-all-features-doc-locked.txt). |
| `env 'RUSTDOCFLAGS=-D warnings' cargo doc --all-features --no-deps --locked` | 0 | Strict Rustdoc generated successfully with warnings denied. Full output: [`final-cargo-doc-strict.txt`](final-cargo-doc-strict.txt). |
| `./.infra/bin/coverage.sh` | 0 | Thresholds met: functions 98.10% (310/316; required ≥95%), lines 95.30% (2616/2745; required >90%), regions 93.59% (4261/4553; required >85%). Full output: [`final-coverage.txt`](final-coverage.txt). |
| `./.infra/bin/ci-check.sh` | 0 | Configured build, test, doc, README, package, coverage, audit, and dependency checks passed. `coverage-cfg-clippy` is disabled by `.infra/ci/ci.toml` (`coverage_cfg_clippy = false`) and was reported by CI as not configured. Coverage: functions 98.10%, lines 95.26%, regions 93.61%. Full output: [`final-ci-check.txt`](final-ci-check.txt). |
| `timeout 300s python3 /data/working/qubit/.worktrees/rs-websocket/ws-r2-20261106-ea19bd63/scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/.worktrees/rs-websocket/ws-r2-20261106-ea19bd63 --output /tmp/rs-websocket-interop-ea19bd63` | 0 | WS 5/5 and WSS 3/3 passed; both source trees were unchanged during the check. Metadata: [`current-interop.json`](current-interop.json); stdout: [`current-interop-command.txt`](current-interop-command.txt). |

## Diagnostic and setup outcomes

- The first focused queue test exited 101 because the worktree's documented sibling path dependencies were absent. The repository's `./.infra/bin/prepare-local-path-dependencies.sh` completed successfully and fetched the exact pinned `rs-config`, `rs-redact`, and `rs-value` revisions; the focused test then passed.
- The first candidate-only compile exited 101 because the new private test helper omitted `std::sync::Mutex`; adding the test-only import fixed it, and both focused selector tests passed.
- The first wrapped baseline probe's Rust test body passed, but a zsh wrapper returned 1 because it assigned to the read-only special variable `status`. The unwrapped probe rerun exited 0; both attempts and their outputs are recorded in `commands.txt` and the baseline logs.
- An initial `rustfmt --check` exited 1 on pre-existing formatting in touched files. The required `align-ci.sh` applied the repository format, then passed. Its only resulting changes were within the two touched Rust files.
- A staged `git diff --cached --check` initially reported trailing blank lines in generated raw log files; only those final blank lines were removed, after which the staged diff check passed. Raw sample lines were unchanged.
