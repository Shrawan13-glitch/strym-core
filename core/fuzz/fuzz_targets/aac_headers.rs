#![no_main]
//! Fuzz target: AAC header parsing. Exercises the ADTS header parser, ASC
//! parse/build, and the ADTS-stripping path against arbitrary bytes. None may
//! panic, even on headers that claim absurd frame lengths.

use libfuzzer_sys::fuzz_target;
use stream::codecs::aac;

fuzz_target!(|data: &[u8]| {
    if let Some(h) = aac::parse_adts(data) {
        // A parsed header must produce a usable ASC.
        let _ = aac::build_asc(&h);
        let asc = aac::adts_to_asc(data);
        if let Some(asc) = asc {
            let _ = aac::parse_asc(&asc);
        }
    }
    // Stripping must be total and panic-free.
    let _ = aac::strip_adts(data);
});
