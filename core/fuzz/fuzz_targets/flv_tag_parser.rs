#![no_main]
//! Fuzz target: incremental FLV tag parser. Arbitrary byte streams must be
//! accepted incrementally without panicking — the parser buffers partial
//! headers/tags and must hold up under any chunking.

use libfuzzer_sys::fuzz_target;
use stream::rtmp::FlvTagParser;

fuzz_target!(|data: &[u8]| {
    let mut p = FlvTagParser::new();
    // Feed one byte at a time: the worst-case chunking for the incremental
    // parser. An Err (bad header) is a value, never a panic.
    for b in data {
        let _ = p.feed(&[*b]);
    }
});
