# Stream Core — Production Readiness Plan

A reusable live-streaming core (Rust, `no_std`-friendly std-only today): captures
time-stamped encoded packets from the platform, owns time/A-V sync, FLV muxing,
backpressure, and pluggable transport (RTMP first).

Current state: clock, bounded backpressure buffer, FLV muxer, H.264/AAC reshaping,
engine pipeline, and an RTMP publish client (complex handshake, chunking, AMF0)
are implemented with 59 unit + 2 integration tests passing.

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

## Phase 2 — Correctness & Protocol Robustness ✅

Goal: survive hostile networks and malformed inputs without panicking.

- [x] Typed, exhaustive error handling; no `unwrap`/`expect` outside tests
  (engine mutex recovery, let-else restructures, poisoning handled as a value)
- [x] RTMP control-plane hardening:
  - `_error` / `onStatus` error-code handling on every transaction
  - Window Acknowledgement Size + acknowledgement replies
  - Ping request/response (protocol control 6)
  - Read/write timeouts on all socket operations (`RtmpConfig::timeout`)
- [x] Timestamp discipline: monotonicity enforcement (DTS clamped within
  tolerance, ordering error beyond), 24→32-bit extension byte, negative
  composition-time safety (signed 24-bit), DTS reordering tolerance
- [x] Bounds-checked parsing everywhere (chunk reader, AMF0 reader, FLV parser,
  ADTS/ASC, Annex-B splitter)
- [x] Fuzz targets (`cargo-fuzz`): chunk reassembly, AMF0 decode, FLV tag parser,
  H.264/AAC header parsers
- [x] Property tests (`proptest`): Annex-B ↔ length-prefixed round-trip, ADTS ↔ ASC

Exit criteria: zero panics under fuzz campaign; all protocol error paths unit-tested.
(Verified locally: 1.5M+ runs/target with no crashes; CI runs a 5s smoke fuzz per target.)

## Phase 3 — Resilience: Reconnect & Recovery ✅

Goal: a dropped connection must not end the stream.

- [x] Auto-reconnect: exponential backoff + jitter, capped retries, session resume
- [x] Re-emit FLV header + sequence headers after every reconnect (server forgets)
- [x] Stall detection (no ack/progress within N ms → reconnect, not hang)
- [x] Drop policies wired to `LatencyProfile` with keyframe-aligned cuts
- [x] Clock-drift compensation across reconnects (no timestamp jump on resume)

Exit criteria: kill-server test passes (stream resumes within budget, viewers resync).
(Verified: `tests/reconnect.rs` kill-server test green — resume on keyframe, headers
re-emitted, timestamps continuous, recovered within budget.)

## Phase 4 — Observability & Telemetry ✅

Goal: know what the stream is doing without attaching a debugger.

- [x] Structured logging, levels configurable by the platform — a std-only facade
  (`Logger` trait + global registry + `log_event!`) instead of `tracing`, honoring
  the no-dependency contract
- [x] Metrics: bitrate out, effective throughput, drop ratio, buffer ms, RTT, reconnects
- [x] QoS event stream (callback-friendly) for platform UI wiring
- [x] No allocations in the hot path for telemetry (preallocated ring)

Exit criteria: a 1-hour stream produces a complete, queryable QoS summary.
(Verified: `engine_emits_qos_samples_into_queryable_summary` drives a full
hour-equivalent window through the real engine path — ring retains 3600 samples,
summary complete and chronological; `qos_summary_spans_real_elapsed_time` checks
the span accumulates over real time.)

## Phase 5 — Feature Completeness ✅

Goal: everything a production broadcaster needs.

- [x] RTMP ingest server (accept publish): handshake, `connect`/`createStream`/
      `publish` control plane, FLV tag-body decode back to `MediaPacket`s feeding
      `PacketSink`s (HLS, recordings) via `PublishHandler`; refuses unknown apps,
      honors chunk-size/ack/ping control traffic, times out silent peers. Unit +
      loopback tests with our own client, and wire-interop with ffmpeg as publisher.
- [x] HLS output: fMP4/CMAF segmenter (`init.mp4` + `segN.m4s`) + `m3u8` media
      playlist with `#EXT-X-VERSION:7`, keyframe-aligned cuts, sliding window,
      atomic file writes (`DirStorage`) + `MemoryStorage`
- [x] Additional transports behind the existing `Transport` trait (SRT first) —
      note: `Fanout` transport (tee publish + local sinks); SRT deferred (protocol
      complexity, no reference tool; ffmpeg has SRT available for future work)
- [x] Recording to file while live publishing (dual transport): `RecordingOutput`
      owns its own muxer so reconnects never corrupt the file
- [x] Fuzz target for the ingest decode path (`rtmp_ingest`: chunk reassembly +
      FLV tag-body decode)

Exit criteria: feature matrix validated by integration tests against reference tools
(ffmpeg/ffprobe round-trip).
(Verified: `tests/hls.rs` ffmpeg→FLV→engine→HLS→ffprobe/ffmpeg decode round-trip;
`tests/rtmp.rs` ffmpeg→RTMP server→HLS→ffprobe/ffmpeg decode round-trip. Test
totals: 132 unit + HLS + pipeline + reconnect + RTMP interop.)

## Phase 6 — Platform Integration & API 1.0 ✅

Goal: mobile/desktop platforms can embed the core safely.

- [x] UniFFI bindings (Kotlin/Swift), ergonomic async API surface
- [x] Memory-ownership contract documented (who frees what, thread-safety)
- [ ] Sample apps: Android publisher, iOS publisher — **moved to a separate
      platform repo** (`stream-platform`): the core stays one repo, platform
      integration another. Android app roadmap tracked in
      `stream-platform/PLAN.md`.
- [x] API review + semver 1.0.0 freeze, `#[non_exhaustive]` where appropriate

Exit criteria (core): API frozen, bindings generated, CI verifies them.
Exit criteria (platform): sample apps stream live to a public RTMP endpoint for
30+ minutes — tracked in the platform repo.

Status: core API frozen at 1.0.0 (`#[non_exhaustive]` on enums, output structs,
and `MediaPacket`; user-constructed config structs stay exhaustive). `stream-ffi`
workspace crate (UniFFI 0.32) wraps the core in a single `StreamSession` object
with lifecycle/stat callbacks, structured-log sink, and full reconnect handling;
bindings generate via `core/ffi/generate-bindings.sh`, CI verifies Kotlin+Swift
generation, and 5 end-to-end tests cover loopback publish, reconnect, connect
failure/retry, bad config, and log routing. The engine is YouTube-Live-compatible
(verified via ffmpeg/ffprobe interop on the ingest path). Sample apps live in the
platform repo (outstanding).

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
