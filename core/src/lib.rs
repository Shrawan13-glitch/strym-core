//! `stream` — a reusable live-streaming core.
//!
//! Design: the platform side (Android/iOS/desktop) captures input and feeds it to this
//! crate as time-stamped media packets. This core owns *time, bytes, and resilience*:
//! A/V sync, FLV muxing, transport, buffering/backpressure. Transport is pluggable
//! (RTMP first, others later), so the core is transport-agnostic.

pub mod backpressure;
pub mod clock;
pub mod codecs;
pub mod engine;
pub mod models;
pub mod mux;
pub mod rtmp;
pub mod sha256;
pub mod transport;
