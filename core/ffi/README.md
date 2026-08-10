# stream-ffi — Kotlin/Swift bindings for the `stream` core

This crate exposes the dependency-free `stream` core to mobile platforms via
[UniFFI]. It ships one object — [`StreamSession`] — that an app drives from
Kotlin or Swift: create it, push encoded frames, watch lifecycle/stats
callbacks, and stop it.

[UniFFI]: https://mozilla.github.io/uniffi-rs/
[`StreamSession`]: https://github.com/stream/stream#readme

## The surface at a glance

| Rust | Kotlin / Swift |
| --- | --- |
| `StreamSession::new(config, listener)` | `StreamSession(config, listener)` |
| `start()` / `stop()` / `retry()` | `start()` / `stop()` / `retry()` |
| `pushVideo` / `pushAudio` | `pushVideo(ptsMs, isKeyframe, annexB)` |
| `stats()` / `state()` / `lastError()` | same |
| `StreamListener` | `StreamListener` interface (state + stats callbacks) |
| `LogSink` | `LogSink` interface (structured logs) |

See `src/lib.rs` for the full contract, including the memory & threading rules.

## Generating the bindings

Bindings are generated, not committed. From `core/`:

```sh
# 1. Build the shared library (generates the scaffolding the bindgen reads).
cargo build -p stream-ffi

# 2. Kotlin (emits uniffi/stream_ffi/stream_ffi.kt).
cargo run -p stream-ffi --bin uniffi-bindgen -- generate \
  --language kotlin --no-format \
  --out-dir path/to/app/src/main/java \
  --library target/debug/libstream_ffi.so

# 3. Swift (emits stream_ffi.swift + stream_ffiFFI.h + modulemap).
cargo run -p stream-ffi --bin uniffi-bindgen -- generate \
  --language swift --no-format \
  --out-dir path/to/ios/App \
  --library target/debug/libstream_ffi.so
```

Or run the bundled script for a scratch copy in `target/bindings/`:

```sh
./generate-bindings.sh
```

## Building for a device

Use the `staticlib` crate type for linking into the app:

```sh
cargo build -p stream-ffi --release --target aarch64-apple-ios  # iOS device
cargo build -p stream-ffi --release --target aarch64-linux-android  # Android arm64
```

## Adding to the API

1. Change the types in `src/lib.rs` (only UniFFI-supported types; see the
   [UniFFI type docs](https://mozilla.github.io/uniffi-rs/latest/types)).
2. `cargo fmt && cargo clippy -p stream-ffi --all-targets -- -D warnings`
3. Add a unit test under `mod tests` and/or an integration test in `tests/`.
4. Regenerate the bindings (above) and eyeball the new surface.

## Notes

- The core keeps its std-only, dependency-free contract: UniFFI lives in this
  crate only.
- `StreamSession` callbacks fire on the session's worker thread; hop to the UI
  thread before touching views.
- A session is single-use: after `stop()` create a new one to go live again.
