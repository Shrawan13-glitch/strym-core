#![no_main]
//! Fuzz target: AMF0 decode. The reader must return values or `None` — never
//! panic — for arbitrary bytes, including truncated strings, hostile objects,
//! and unknown type markers.

use libfuzzer_sys::fuzz_target;
use stream::rtmp::amf0;

fuzz_target!(|data: &[u8]| {
    let mut r = amf0::Reader::new(data);
    let vals = r.read_all();
    for v in &vals {
        // Forcing a full walk of the value tree exercises every nested reader.
        let _ = format!("{v:?}");
    }
});
