#![no_main]
//! Fuzz target: RTMP ingest decode. Reassembles arbitrary bytes into chunk
//! messages via `ChunkReader`, then pushes every audio/video message through
//! the FLV tag-body decoder — the exact decode path the ingest server runs on
//! a publisher's traffic. None of it may panic on hostile input; malformed
//! bytes surface as `Err` / `None`, which is the server's normal handling.

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use stream::flv;
use stream::rtmp::ChunkReader;

fuzz_target!(|data: &[u8]| {
    let mut reader = ChunkReader::new();
    let mut cur = Cursor::new(data);
    loop {
        let msg = match reader.read_message(&mut cur) {
            Ok(m) => m,
            Err(_) => break,
        };
        // Media messages take the server's decode path: bounds-checked FLV
        // tag-body decoding into packets/configs.
        if matches!(msg.mtype, flv::TAG_AUDIO | flv::TAG_VIDEO) {
            let _ = flv::decode_tag(msg.mtype, msg.ts, &msg.payload);
        }
    }
});
