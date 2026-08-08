# Stream Core — Production Readiness Plan

A reusable live-streaming core (Rust, `no_std`-friendly std-only today): captures
time-stamped encoded packets from the platform, owns time/A-V sync, FLV muxing,
backpressure, and pluggable transport (RTMP first).

Current state: clock, bounded backpressure buffer, FLV muxer, H.264/AAC reshaping,
engine pipeline, and an RTMP publish client (complex handshake, chunking, AMF0)
are implemented with 23 unit tests passing.

## Phase 1 — Engineering Foundation & Hardening ✅

Goal: the repo must enforce quality, not rely on discipline.

- [x] Version control: `git init`, `.gitignore` (no build artifacts, no secrets)
- [x] Reproducible toolchain: `rust-toolchain.toml` (pinned stable)
- [x] Consistent style: `rustfmt.toml`, entire tree formatted
- [x] Zero-warning policy: lints in `Cargo.toml`, `unsafe_code` forbidden
- [x] All existing clippy warnings fixed
- [x] E2E integration test: full pipeline (engine → muxer → FLV) validated by a demuxer
- [x] CI: format check, clippy `-D warnings`, tests (debug + release)
- [x] `AGENTS.md` with canonical commands

Exit criteria: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test --all-targets` all green in CI and locally.

## Phase 2 — Correctness & Protocol Robustness

Goal: survive hostile networks and malformed inputs without panicking.

- Typed, exhaustive error handling; no `unwrap`/`expect` outside tests
- RTMP control-plane hardening:
  - `_error` / `onStatus` error-code handling on every transaction
  - Window Acknowledgement Size + acknowledgement replies
  - Ping request/response (protocol control 6)
  - Read/write timeouts on all socket operations
- Timestamp discipline: monotonicity enforcement, 24→32-bit extension, negative
  composition-time safety, DTS reordering tolerance
- Bounds-checked parsing everywhere (chunk reader, AMF0 reader, FLV parser)
- Fuzz targets (`cargo-fuzz`): chunk reassembly, AMF0 decode, FLV tag parser,
  H.264/AAC header parsers
- Property tests (`proptest`): Annex-B ↔ length-prefixed round-trip, ADTS ↔ ASC

Exit criteria: zero panics under 24h fuzz campaign; all protocol error paths unit-tested.

## Phase 3 — Resilience: Reconnect & Recovery

Goal: a dropped connection must not end the stream.

- Auto-reconnect: exponential backoff + jitter, capped retries, session resume
- Re-emit FLV header + sequence headers after every reconnect (server forgets)
- Stall detection (no ack/progress within N ms → reconnect, not hang)
- Drop policies wired to `LatencyProfile` with keyframe-aligned cuts
- Clock-drift compensation across reconnects (no timestamp jump on resume)

Exit criteria: kill-server test passes (stream resumes within budget, viewers resync).

## Phase 4 — Observability & Telemetry

Goal: know what the stream is doing without attaching a debugger.

- Structured logging (`tracing`), levels configurable by the platform
- Metrics: bitrate out, effective throughput, drop ratio, buffer ms, RTT, reconnects
- QoS event stream (callback-friendly) for platform UI wiring
- No allocations in the hot path for telemetry (preallocated ring)

Exit criteria: a 1-hour stream produces a complete, queryable QoS summary.

## Phase 5 — Feature Completeness

Goal: everything a production broadcaster needs.

- RTMP ingest server (accept publish) — if product requires server-side
- HLS output: segmenter + `m3u8` playlists (LL-HLS later)
- Additional transports behind the existing `Transport` trait (SRT first)
- Recording to file while live publishing (dual transport)

Exit criteria: feature matrix validated by integration tests against reference tools
(ffmpeg/ffprobe round-trip).

## Phase 6 — Platform Integration & API 1.0

Goal: mobile/desktop platforms can embed the core safely.

- UniFFI bindings (Kotlin/Swift), ergonomic async API surface
- Memory-ownership contract documented (who frees what, thread-safety)
- Sample apps: Android publisher, iOS publisher
- API review + semver 1.0.0 freeze, `#[non_exhaustive]` where appropriate

Exit criteria: sample apps stream live to a public RTMP endpoint for 30+ minutes.

## Phase 7 — Security & Supply Chain

Goal: no CVEs, no secrets leaks, no license surprises.

- `cargo-deny` (bans, licenses, advisories) + `cargo-audit` in CI
- Input size caps (max tag size, max chunk count, max AMF0 depth)
- Threat model: malicious server replies, truncated streams, oversized messages
- Secret hygiene: stream keys never logged, never in error messages

Exit criteria: deny/audit clean; threat-model checklist signed off.

## Phase 8 — Performance & Release Engineering

Goal: measurable headroom and repeatable releases.

- `criterion` benchmarks: mux throughput, chunk framing, AMF0 encode/decode
- Zero-copy hot path review (avoid per-tag allocations in muxer/transport)
- Profiling under 1080p60 + 320kbps audio load on a low-end phone CPU profile
- Release pipeline: tagged releases, changelog (keep a changelog), cross-compilation
  targets (aarch64-android, aarch64-ios, x86_64-linux)

Exit criteria: benchmark regression gate in CI; release artifacts built by CI.

## Working agreements (all phases)

- Trunk quality gate: fmt clean, clippy `-D warnings`, tests green — enforced by CI
- Every behavior change ships with a test; no test, no merge
- Errors are values, panics are bugs
- No new dependency without justification recorded in the PR/commit
