cargo test --locked --release --lib -- --list --ignored exit=0
timeout 300s cargo test --locked --release --lib wake_probe -- --ignored --nocapture --test-threads=1 exit=0
timeout 300s cargo test --locked --release --lib queue_followup_probe -- --ignored --nocapture --test-threads=1 exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe -- --ignored --nocapture --test-threads=1 exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe -- --ignored --nocapture --test-threads=1 [candidate] exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe -- --ignored --nocapture --test-threads=1 [optimized candidate] exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe -- --ignored --nocapture --test-threads=1 [thresholded candidate] exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe -- --ignored --nocapture --test-threads=1 [final candidate] exit=0
cd /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-baseline-20261105 && cargo test --locked --release --lib capacity_release_probe --no-run exit=0
round=01 profile=baseline exit=0
round=01 profile=candidate exit=0
round=02 profile=baseline exit=0
round=02 profile=candidate exit=0
round=03 profile=baseline exit=0
round=03 profile=candidate exit=0
round=04 profile=baseline exit=0
round=04 profile=candidate exit=0
round=05 profile=baseline exit=0
round=05 profile=candidate exit=0
round=06 profile=baseline exit=0
round=06 profile=candidate exit=0
round=07 profile=baseline exit=0
round=07 profile=candidate exit=0
timeout 300s cargo test --locked --release --lib registration_probe -- --ignored --nocapture --test-threads=1 [baseline] exit=0
timeout 300s cargo test --locked --release --lib registration_probe -- --ignored --nocapture --test-threads=1 [candidate] exit=0
baseline cargo test --locked --release --lib capacity_release_probe_multithread --no-run exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe_multithread -- --ignored --nocapture --test-threads=1 [baseline] exit=101
timeout 300s cargo test --locked --release --lib capacity_release_probe_multithread -- --ignored --nocapture --test-threads=1 [baseline retry after probe cleanup fix] exit=0
timeout 300s cargo test --locked --release --lib capacity_release_probe_multithread -- --ignored --nocapture --test-threads=1 [candidate] exit=0
cargo test --locked --lib session::tests::internal_tests -- --test-threads=1 exit=101
cargo test --locked --lib session::tests::internal_tests::test_capacity_index_reset_waiter_can_be_selected_again -- --exact --nocapture exit=101
cargo test --locked --lib session::tests::internal_tests::test_capacity_index_reset_waiter_can_be_selected_again -- --exact --nocapture [fixed scenario] exit=0
./.infra/bin/align-ci.sh exit=1
baseline worktree ./.infra/bin/style-check.sh exit=1
./.infra/bin/align-ci.sh [after formatting/new Rustdoc fixes] exit=1
./.infra/bin/align-ci.sh [final source pass] exit=1
./.infra/bin/style-check.sh [after in-scope doc fixes] exit=1
./.infra/bin/style-check.sh [final after local fixes] exit=1
./.infra/bin/ci-check.sh exit=1
cargo test --locked exit=0
cargo test --all-features --locked exit=0
cargo test --all-features --locked --doc exit=0
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked exit=0
./.infra/bin/coverage.sh exit=0
cargo test --locked --lib session::tests::internal_tests -- --test-threads=1 [final] exit=0
timeout 300s cargo test --locked --release --lib test_capacity_release_after_unfit_prefix_probe -- --ignored --nocapture --test-threads=1 exit=0
timeout 300s cargo test --locked --release --lib test_multithread_capacity_release_progress_probe -- --ignored --nocapture --test-threads=1 exit=0
baseline 6c2ab2f ./.infra/bin/style-check.sh [clean source] exit=1
timeout 300s cargo test --locked --release --lib wake_probe -- --ignored --nocapture --test-threads=1 [candidate] exit=0
timeout 300s cargo test --locked --release --lib wake_probe -- --ignored --nocapture --test-threads=1 [after p99 correction] exit=0
timeout 300s cargo test --locked --release --lib queue_followup_probe -- --ignored --nocapture --test-threads=1 [candidate] exit=0
timeout 300s python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-20261105 --output /tmp/rs-websocket-interop-20261105T060550147Z-5656d7a6 exit=1
timeout 300s python3 /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-20261105/scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-20261105 --output /tmp/rs-websocket-interop-20261105T060550147Z-5656d7a6-rerun exit=0
./.infra/bin/ci-check.sh [retry after fixing rules in changed files] exit=1
./.infra/bin/ci-check.sh --only clippy exit=1
cargo test --locked --lib session::internal::bounded_queue::tests::test_waiter_index_deactivates_after_contention_peak_recedes -- --exact --nocapture exit=0

Final post-recovery checks (all exit 0):
./.infra/bin/align-ci.sh [final formatted tree] exit=0
./.infra/bin/style-check.sh [final source + added multi-thread probe] exit=0
cargo test --locked [final] exit=0
cargo test --all-features --locked [final] exit=0
cargo test --all-features --locked --doc [final] exit=0
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps --locked [final] exit=0
./.infra/bin/coverage.sh [final source module layout] exit=0; functions=97.28%, lines=94.72%, regions=92.84%
./.infra/bin/ci-check.sh --only clippy [after collapsible-if correction] exit=0
./.infra/bin/ci-check.sh [after final multi-thread probe + clippy correction] exit=0; coverage functions=97.28%, lines=94.68%, regions=92.86%
cargo test --locked --lib session::tests::internal_tests -- --test-threads=1 [final restored tests] exit=0; 25 passed, 6 ignored
 timeout 300s cargo test --locked --release --lib test_capacity_release_after_unfit_prefix_probe -- --ignored --nocapture --test-threads=1 [final] exit=0
timeout 300s cargo test --locked --release --lib test_manual_multi_thread_capacity_release_contention_probe -- --ignored --nocapture --test-threads=1 [candidate final] exit=0
 timeout 300s cargo test --locked --release --lib test_manual_multi_thread_queue_follow_up_progress_probe -- --ignored --nocapture --test-threads=1 [candidate final] exit=0
timeout 300s python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-20261105 --output /tmp/rs-websocket-interop-20261105T060550147Z-5656d7a6-final2 exit=0; WS=5, WSS=3, source hashes unchanged

Diagnosed non-final failures:
./.infra/bin/style-check.sh [clean HEAD before validation repair] exit=1; pre-existing Rustdoc/test naming/path/format issues; fixed without changing rule strength
./.infra/bin/ci-check.sh [style baseline repaired, before test module relocation] exit=1; coverage functions=94.22%; internal queue tests had been disconnected by removing the old #[path] entry; restored them under a standard `src/session/tests` module and reran coverage/CI successfully
./.infra/bin/ci-check.sh --only clippy [initial final multi-thread probe] exit=1; clippy::collapsible_if in the new test helper; collapsed the condition and reran clippy plus full CI successfully
cargo test --locked --lib session::tests::internal_tests [one run concurrent with rs-infra cleanup] exit=101; target/debug artifact directory was removed by the parallel style-check cleanup; rerun after cleanup completed exit=0
timeout 300s python3 scripts/check-current-interop.py --web /data/working/qubit/rust-common/rs-web --websocket /data/working/qubit/rust-common/.worktrees/rs-websocket-ws001-ws002-20261105 --output /tmp/rs-websocket-interop-20261105T060550147Z-5656d7a6-final3 [final source tree hash] exit=0; WS=5, WSS=3

timeout 300s cargo test --locked --release --lib test_manual_multi_thread_capacity_release_contention_probe -- --ignored --nocapture --test-threads=1 [post-clippy-fix final] exit=0; candidate release p50=4,107 ns, baseline p50=134,119 ns
