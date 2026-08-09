#![no_main]
//! Fuzz target: H.264 header parsing. Exercises the Annex-B splitter, SPS/PPS
//! extraction, AVCC building, and the length-prefixed round-trip against
//! arbitrary bytes. None of these may panic.

use libfuzzer_sys::fuzz_target;
use stream::codecs::h264;

fuzz_target!(|data: &[u8]| {
    // Split arbitrary bytes into NAL units (bounds-checked).
    let nals = h264::split_annex_b(data);
    // Rebuild a length-prefixed payload from whatever came out.
    let lp = h264::to_length_prefixed(&nals);
    // Parse it back; must not panic even if lengths overflow.
    let _ = h264::from_length_prefixed(&lp);
    // Whole-packet conversions.
    let _ = h264::annexb_to_length_prefixed(data);
    let _ = h264::annexb_to_avcc(data);
    let _ = h264::contains_keyframe(data);
    let _ = h264::extract_sps_pps(data);
});
