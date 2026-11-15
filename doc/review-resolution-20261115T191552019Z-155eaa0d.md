# Consumer compatibility implementation evidence

Date: 2026-11-15

This record supports WS-R01, WS-R02, WS-R03, and the WS-V01 source check. The
design and implementation plan are retained separately and are not included in
this implementation's commits.

## Initial workspace state

- Source repository: `/data/working/qubit/rust-common/rs-websocket`
- Initial branch and HEAD: `dev-starfish`, `8a728bf3c97b363e65c3c8e1b9ee9cc137c455f3`
- Existing changes: `Cargo.toml`, `Cargo.lock`, and 40 files under
  `vendor/tungstenite/`; the two task design documents were also untracked.
- The manifest change adds `vendor/tungstenite/**` to the package and patches
  crates.io `tungstenite` to that local source. The lockfile removes the
  registry source and checksum for the patched package. These changes and the
  vendored source were reviewed and carried into the registered task worktree;
  no files were removed from the source workspace.
- Vendor file manifest and SHA-256 entries are recorded outside the repository
  in the task's external evidence records.

## WS-V01 source and behavior check

The package metadata identifies the vendored code as `tungstenite` 0.30.0,
originally from the snapview repository. Its normalized `Cargo.toml` and
`.cargo_vcs_info.json` are Cargo package metadata; they are not byte-for-byte
upstream repository files. Cargo's source cache contains the requested Qubit
fork checkout, and its HEAD is the complete pin
`b24a4442be0247a71fe9ee4e520eb483ac1b2873`. The commit's parent is
`eae89b73ad2736e5bb4444bb9ad75c5466f14ac2`; the pin adds an explicit test for
rejecting non-ASCII `Connection` values.

Compared with the cached fork at the pinned commit, the vendored source files
match except for `src/handshake/client.rs` and expected package metadata files.
The handshake implementation now uses the same `header_contains_token`
algorithm as the fork: it examines every `Connection` field, splits comma
lists, trims HTTP whitespace, compares the complete token without case, and
rejects a non-text field value. The vendored version also has a focused
regression test for an invalid field followed by a separate `Upgrade` field.
Before that test and correction, the vendor's `filter_map` skipped the invalid
field and accepted the response; the pinned fork rejects it. The package
metadata differences are Cargo normalization/packaging differences, not
protocol code changes.

`cargo info tungstenite` resolved the current registry package to 0.30.0.
The independent `baseline` probe confirms that this registry version does not
implement the full token contract. No compatible published version was
identified, so the fixed commit remains the consumer patch. Cargo's Git fetch
for the real patched consumer supplied the exact source checkout; GitHub's
archive endpoint separately returned HTTP 403, so no archive-based comparison
is claimed.

Decision: WS-V01 found a narrow behavior mismatch in the existing vendor
source; it was corrected to match the pinned fork and covered by the regression
test. No other vendor files were replaced.

## Consumer probe evidence

The probe tests cover patched manifest generation, exact package manifest
identity, exact fork URL and revision, rejection of registry and unrelated Git
sources, explicit test input for an unpacked crate, no false Git identity for
an unpacked crate nested below another repository, and propagation of failing
metadata/test subprocesses. Existing `baseline`, `current`, and
`--expect-fixed` behavior remains covered.

Evidence directories are outside the task worktree:

- `source-patched/consumer-registry.json`: patched source consumer passed; its
  metadata resolved `qubit-websocket -> tokio-tungstenite -> tungstenite` to
  the exact fork commit.
- `baseline/consumer-registry.json`: registry 0.30.0 reproduced the historical
  limitation and the baseline-specific assertions passed.
- `vendor-sha256.txt`: pre-review package file digests.

The handshake fixture covers a single `Upgrade`, both comma-separated token
orders, case and whitespace, both duplicate-field orders, missing `Connection`,
`keep-alive` alone, and substring lookalikes `x-upgrade` and `upgrader`.

## Archived interoperability notes

These are historical local-checkout results and do not establish compatibility
with a published dependency:

- Earlier on 2026-11-02, five interoperability scenarios passed in an external
  consumer against the local `rs-web` checkout at
  `7bd9a4f1f10f4de6051d771f2b5516fc5f193ef2`. The copied test source had SHA-256
  `f1288dce9088c73eba8b6bb73fa904968a9db4b0f6ba739f06a2de9829fcf73d`.
- The same five scenarios were rerun against the then-current local checkout
  at `74331ed0697042e16e60d3ba6b845f5bfc4937ad`; all passed.
- At that time, the crate's default `qubit-web` Git pin was
  `3046fa0fbc46c6c4584fdf9710b45649dfebd70a`.

This historical information was moved out of the current dependency
architecture description; the entries above are not results from this task's
consumer probe.
