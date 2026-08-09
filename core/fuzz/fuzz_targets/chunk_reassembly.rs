#![no_main]
//! Fuzz target: RTMP chunk-stream reassembly. Feeds arbitrary bytes to the
//! `ChunkReader` and must never panic, no matter how the chunk boundaries,
//! format bits, extended timestamps, or length fields are scrambled.

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use stream::rtmp::ChunkReader;

fuzz_target!(|data: &[u8]| {
    let mut reader = ChunkReader::new();
    let mut cur = Cursor::new(data);
    // Keep pulling messages until the input runs out or an error is surfaced;
    // an error (EOF, bad chunk id, zero chunk size) is a value, never a panic.
    loop {
        let msg = match reader.read_message(&mut cur) {
            Ok(m) => m,
            Err(_) => break,
        };
        // Touch the payload so reassembly actually happened.
        let _ = msg.mtype;
        let _ = msg.payload.len();
    }
});
