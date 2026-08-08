# AGENTS.md

Rust live-streaming core. The Cargo project lives in `core/` (crate `stream`,
edition 2021, std-only, no dependencies). All commands below run from `core/`.

## Canonical commands

```sh
cargo fmt --check       # formatting must be clean (rustfmt.toml: 120 cols)
cargo clippy --all-targets -- -D warnings   # zero warnings required
cargo test --all-targets                   # unit + integration tests
cargo test --release                       # release-mode tests
```

Run `cargo fmt` before committing if `cargo fmt --check` fails.

## Quality gate (enforced by CI)

- Formatting clean
- Clippy `-D warnings` clean
- All tests green (debug + release)
- Every behavior change ships with a test; no test, no merge

## Conventions

- No `unsafe` (forbidden by lint policy); errors are values, panics are bugs
- No `unwrap`/`expect` outside tests and examples
- No new dependency without justification
- Public API must be documented (`missing_docs` is enforced)
- Pinned toolchain in `rust-toolchain.toml`; use it, don't override

## Layout

- `src/engine.rs` — pipeline entry point (buffer → muxer → transport)
- `src/backpressure.rs` — bounded drop-oldest buffer
- `src/mux.rs` — FLV muxer + AMF0 metadata
- `src/codecs/` — H.264 (Annex-B ↔ length-prefixed, SPS/PPS) and AAC (ADTS ↔ ASC)
- `src/transport.rs` — `Transport` trait, `FileTransport`
- `src/rtmp/` — RTMP publish client (handshake, chunking, AMF0)
- `tests/pipeline.rs` — end-to-end engine → FLV validation
- `examples/` — `flv_demo`, `rtmp_demo`, `dump_hs`

## Plan

Roadmap and phase status: see `PLAN.md` at the repo root.
