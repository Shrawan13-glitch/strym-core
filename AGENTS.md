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

## Fuzzing (cargo-fuzz)

The fuzz workspace lives in `core/fuzz/` (6 targets: chunk reassembly, AMF0
decode, FLV tags, H.264 headers, AAC headers, RTMP ingest decode). Needs a clang
toolchain and nightly rust. On this machine: clang is at `/opt/clang-18/bin`,
and libc++ is used instead of libstdc++ (no g++ installed).

```sh
export PATH="/opt/clang-18/bin:$PATH"
export CC="/opt/clang-18/bin/clang" CXX="/opt/clang-18/bin/clang++"
export CXXFLAGS="-stdlib=libc++" CXXSTDLIB="c++"
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="/opt/clang-18/bin/clang++"
export LD_LIBRARY_PATH="/opt/clang-18/lib/x86_64-unknown-linux-gnu:$LD_LIBRARY_PATH"

cargo +nightly fuzz build                      # compile all targets
cargo +nightly fuzz run <target> -- -max_total_time=60
```

Fuzzer-generated files (`fuzz/target/`, `fuzz/corpus/`, `fuzz/artifacts/`) are
git-ignored. CI runs a 5s smoke fuzz per target. `proptest` property tests
(codec round-trips) run under the normal `cargo test`.

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
- `src/flv.rs` — FLV tag-body decoding (inverse of the muxer)
- `src/transport.rs` — `Transport` trait, `FileTransport`, `Fanout`
- `src/sink.rs` — `PacketSink` trait, `RecordingOutput` (file while live)
- `src/rtmp/` — RTMP publish client (handshake, chunking, AMF0) and ingest
  server (`server.rs`: accept publish → `MediaPacket`s → `PacketSink`s)
- `src/hls/` — fMP4/CMAF segmenter + m3u8 playlists (`DirStorage`/`MemoryStorage`)
- `tests/pipeline.rs` — end-to-end engine → FLV validation
- `fuzz/` — cargo-fuzz targets (chunk reassembly, AMF0, FLV, H.264/AAC, RTMP ingest)
- `examples/` — `flv_demo`, `rtmp_demo`, `dump_hs`

## Plan

Roadmap and phase status: see `PLAN.md` at the repo root.
