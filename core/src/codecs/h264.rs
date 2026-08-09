//! H.264 helpers: splitting Annex-B streams, building the AVCC record,
//! converting NAL units to the length-prefixed form FLV requires, and reading
//! the SPS for the stream dimensions.

/// Number of NAL unit types: slice, IDR, SPS, PPS, AUD...
mod nal_type {
    pub const IDR: u8 = 5; // instantaneous decoder refresh = keyframe
    pub const SPS: u8 = 7; // sequence parameter set
    pub const PPS: u8 = 8; // picture parameter set
}

/// Extract the NAL unit type from a NAL header byte.
fn nal_type(byte: u8) -> u8 {
    byte & 0x1F
}

/// Split an Annex-B stream into NAL units (start codes stripped).
///
/// Handles both 3-byte (`00 00 01`) and 4-byte (`00 00 00 01`) start codes, and
/// **groups of consecutive start codes** (some encoders emit a redundant
/// trailing start code before the real one). NAL payloads run between a start
/// code group's end and the next group's start.
pub fn split_annex_b(data: &[u8]) -> Vec<&[u8]> {
    // Locate every run of consecutive start codes as (start, end) pairs.
    let mut groups: Vec<(usize, usize)> = Vec::new();
    let mut i = 0usize;
    while i + 3 < data.len() {
        if data[i] == 0 && data[i + 1] == 0 {
            let mut j = i;
            let mut found = false;
            while j + 3 < data.len() && data[j] == 0 && data[j + 1] == 0 {
                if data[j + 2] == 1 {
                    j += 3;
                    found = true;
                } else if data[j + 2] == 0 && data[j + 3] == 1 {
                    // 4-byte start code `00 00 00 01`.
                    // (Require the 3rd byte to be 0 so that emulation-prevention
                    // bytes `00 00 03 01` are not mistaken for a start code.)
                    j += 4;
                    found = true;
                } else {
                    break;
                }
            }
            if found {
                groups.push((i, j));
                i = j;
                continue;
            }
        }
        i += 1;
    }

    let mut nals = Vec::new();
    // A NAL lives between the end of one group and the start of the next.
    for w in groups.windows(2) {
        let s = w[0].1;
        let mut end = w[1].0;
        while end > s && data[end - 1] == 0 {
            end -= 1;
        }
        if end > s {
            nals.push(&data[s..end]);
        }
    }
    // Any trailing bytes after the last group (the final NAL with no trailer).
    if let Some(last) = groups.last() {
        let s = last.1;
        if s < data.len() {
            let mut end = data.len();
            // trim trailing zero padding, then a trailing bare start code
            while end > s && data[end - 1] == 0 {
                end -= 1;
            }
            while end >= s + 3 && data[end - 3..end] == [0, 0, 1] {
                end -= 3;
            }
            while end >= s + 4 && data[end - 4..end] == [0, 0, 0, 1] {
                end -= 4;
            }
            if end > s {
                nals.push(&data[s..end]);
            }
        }
    }
    nals
}

/// Pull the SPS and PPS from a stream. Returns `(sps, pps)` if both found.
pub fn extract_sps_pps(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut sps = None;
    let mut pps = None;
    for nal in split_annex_b(data) {
        let t = nal_type(nal[0]);
        match t {
            nal_type::SPS => sps = Some(nal.to_vec()),
            nal_type::PPS => pps = Some(nal.to_vec()),
            _ => {}
        }
        if sps.is_some() && pps.is_some() {
            return Some((sps?, pps?));
        }
    }
    None
}

/// True if an Annex-B packet contains an IDR slice (i.e., is a keyframe).
/// Scans all NALs because the config packet carries SPS/PPS before the IDR.
pub fn contains_keyframe(data: &[u8]) -> bool {
    split_annex_b(data).iter().any(|nal| nal_type(nal[0]) == nal_type::IDR)
}

/// Build an `AVCDecoderConfigurationRecord` from SPS/PPS. This is what goes in
/// the FLV video-sequence header and tells every player how to decode. Profile
/// and level are read from the SPS so the record always matches the stream.
pub fn build_avcc(sps: &[u8], pps: &[u8]) -> Vec<u8> {
    let profile = *sps.get(1).unwrap_or(&0x64);
    let compatibility = *sps.get(2).unwrap_or(&0);
    let level = *sps.get(3).unwrap_or(&0x1F);

    let mut out = Vec::with_capacity(6 + sps.len() + pps.len());
    out.push(0x01); // configurationVersion
    out.push(profile);
    out.push(compatibility);
    out.push(level);
    out.push(0xFF); // lengthSizeMinusOne = 3 -> 4-byte NAL lengths
    out.push(0xE1); // numOfSequenceParameterSets
    out.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    out.extend_from_slice(sps);
    out.push(0x01); // numOfPictureParameterSets
    out.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    out.extend_from_slice(pps);
    out
}

/// Convert NAL units to FLV's length-prefixed form (4-byte big-endian length
/// then the unit). NALs must not include start codes.
pub fn to_length_prefixed(nals: &[&[u8]]) -> Vec<u8> {
    let total: usize = nals.iter().map(|n| n.len() + 4).sum();
    let mut out = Vec::with_capacity(total);
    for nal in nals {
        out.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        out.extend_from_slice(nal);
    }
    out
}

/// Reshape an entire Annex-B packet into one length-prefixed FLV payload.
pub fn annexb_to_length_prefixed(data: &[u8]) -> Vec<u8> {
    let nals = split_annex_b(data);
    to_length_prefixed(&nals)
}

/// Parse length-prefixed NAL units (FLV's wire form) back into NAL slices.
/// Returns `None` when the buffer is truncated, a length field overruns the
/// buffer, or a NAL length is zero (malformed). Never panics on any input.
pub fn from_length_prefixed(data: &[u8]) -> Option<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        let len_bytes = data.get(pos..pos + 4)?;
        let len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        if len == 0 {
            return None;
        }
        pos += 4;
        let nal = data.get(pos..pos + len)?;
        out.push(nal);
        pos += len;
    }
    Some(out)
}

/// One-shot helper: extract SPS/PPS from an Annex-B packet and build the AVCC
/// record. Returns `None` when the packet lacks SPS or PPS.
pub fn annexb_to_avcc(data: &[u8]) -> Option<Vec<u8>> {
    extract_sps_pps(data).map(|(sps, pps)| build_avcc(&sps, &pps))
}

/// Assemble NAL units (without start codes) into one Annex-B packet with
/// 4-byte start codes — the inverse of [`split_annex_b`].
pub fn to_annex_b(nals: &[&[u8]]) -> Vec<u8> {
    let total: usize = nals.iter().map(|n| n.len() + 4).sum();
    let mut out = Vec::with_capacity(total);
    for nal in nals {
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(nal);
    }
    out
}

/// A bit reader over a byte slice with bounds checking on every read — an SPS
/// is untrusted input, so running off the end yields `None`, never a panic.
struct BitReader<'a> {
    data: &'a [u8],
    /// Bit position from the start of `data`.
    pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn bit(&mut self) -> Option<u32> {
        let byte = *self.data.get(self.pos / 8)?;
        let b = u32::from((byte >> (7 - (self.pos % 8))) & 1);
        self.pos += 1;
        Some(b)
    }

    fn bits(&mut self, n: u32) -> Option<u32> {
        let mut v = 0u32;
        for _ in 0..n {
            v = (v << 1) | self.bit()?;
        }
        Some(v)
    }

    /// Unsigned Exp-Golomb (ITU-T H.264 9.1).
    fn ue(&mut self) -> Option<u32> {
        let mut leading = 0u32;
        while self.bit()? == 0 {
            leading += 1;
            if leading > 31 {
                return None; // pathological; a real SPS never gets here
            }
        }
        if leading == 0 {
            return Some(0);
        }
        let suffix = self.bits(leading)?;
        Some((1u32 << leading) - 1 + suffix)
    }

    /// Signed Exp-Golomb (ITU-T H.264 9.1.1).
    fn se(&mut self) -> Option<i32> {
        let k = self.ue()?;
        let mag = k.div_ceil(2).cast_signed();
        Some(if k % 2 == 1 { mag } else { -mag })
    }
}

/// Profiles that carry the extra chroma/bit-depth/scaling fields in the SPS.
fn is_high_profile(profile: u8) -> bool {
    matches!(profile, 100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128)
}

/// Parse an `AVCDecoderConfigurationRecord`, returning the first SPS and PPS
/// it carries. Returns `None` on any truncation or structural violation.
pub fn extract_sps_pps_from_avcc(avcc: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
    if avcc.len() < 7 || avcc[0] != 1 {
        return None;
    }
    let sps_count = usize::from(avcc[5] & 0x1F);
    let mut pos = 6usize;
    let mut sps = None;
    for _ in 0..sps_count {
        let len = u16::from_be_bytes([*avcc.get(pos)?, *avcc.get(pos + 1)?]) as usize;
        pos += 2;
        let nal = avcc.get(pos..pos + len)?;
        if sps.is_none() {
            sps = Some(nal.to_vec());
        }
        pos += len;
    }
    let pps_count = usize::from(*avcc.get(pos)?);
    pos += 1;
    let mut pps = None;
    for _ in 0..pps_count {
        let len = u16::from_be_bytes([*avcc.get(pos)?, *avcc.get(pos + 1)?]) as usize;
        pos += 2;
        let nal = avcc.get(pos..pos + len)?;
        if pps.is_none() {
            pps = Some(nal.to_vec());
        }
        pos += len;
    }
    Some((sps?, pps?))
}

/// Parse the coded width and height out of a sequence parameter set (SPS NAL
/// payload, header byte included). Returns `None` on any truncation or
/// syntactic surprise — dimensions are informational, never worth a panic.
pub fn parse_sps_dimensions(sps: &[u8]) -> Option<(u32, u32)> {
    let mut r = BitReader::new(sps);
    let header = r.bits(8)?;
    if nal_type(header as u8) != nal_type::SPS {
        return None;
    }
    let profile = r.bits(8)? as u8;
    r.bits(8)?; // constraint flags
    r.bits(8)?; // level
    r.ue()?; // seq_parameter_set_id

    let mut chroma_format_idc = 1u32;
    if is_high_profile(profile) {
        chroma_format_idc = r.ue()?;
        if chroma_format_idc == 3 {
            r.bit()?; // separate_colour_plane_flag
        }
        r.ue()?; // bit_depth_luma_minus8
        r.ue()?; // bit_depth_chroma_minus8
        r.bit()?; // qpprime_y_zero_transform_bypass_flag
        let scaling_present = r.bit()?;
        if scaling_present == 1 {
            let count = if chroma_format_idc == 3 { 12 } else { 6 };
            for i in 0..count {
                let present = r.bit()?;
                if present == 1 {
                    let size = if i < 6 { 16 } else { 64 };
                    // Skip the scaling list (H.264 7.3.2.1.1.1).
                    let mut last = 8i32;
                    let mut next = 8i32;
                    for _ in 0..size {
                        if next != 0 {
                            let delta = r.se()?;
                            next = (last + delta + 256) % 256;
                        }
                        last = if next == 0 { last } else { next };
                    }
                }
            }
        }
    }

    r.ue()?; // log2_max_frame_num_minus4
    let poc_type = r.ue()?;
    match poc_type {
        0 => {
            r.ue()?; // log2_max_pic_order_cnt_lsb_minus4
        }
        1 => {
            r.bit()?; // delta_pic_order_always_zero_flag
            r.se()?; // offset_for_non_ref_pic
            r.se()?; // offset_for_top_to_bottom_field
            let cycle = r.ue()?;
            if cycle > 255 {
                return None;
            }
            for _ in 0..cycle {
                r.se()?;
            }
        }
        _ => {}
    }
    r.ue()?; // max_num_ref_frames
    r.bit()?; // gaps_in_frame_num_value_allowed_flag
    let width_mbs = r.ue()? + 1;
    let height_map_units = r.ue()? + 1;
    let frame_mbs_only = r.bit()?;
    if frame_mbs_only == 0 {
        r.bit()?; // mb_adaptive_frame_field_flag
    }
    r.bit()?; // direct_8x8_inference_flag

    let mut crop_left = 0u32;
    let mut crop_right = 0u32;
    let mut crop_top = 0u32;
    let mut crop_bottom = 0u32;
    if r.bit()? == 1 {
        crop_left = r.ue()?;
        crop_right = r.ue()?;
        crop_top = r.ue()?;
        crop_bottom = r.ue()?;
    }

    // Crop units depend on the chroma format (H.264 7.4.2.1.1); 4:2:0 is by
    // far the dominant case for streaming.
    let (unit_x, unit_y) = match chroma_format_idc {
        1 => (2, 2 * (2 - frame_mbs_only)),
        2 => (2, 2 - frame_mbs_only),
        // 0 (monochrome) and anything exotic.
        _ => (1, 2 - frame_mbs_only),
    };
    let width = width_mbs * 16 - (crop_left + crop_right) * unit_x;
    let height = (2 - frame_mbs_only) * height_map_units * 16 - (crop_top + crop_bottom) * unit_y;
    if width == 0 || height == 0 {
        return None;
    }
    Some((width, height))
}

#[cfg(test)]
mod tests {
    use super::*;

    // hand-built SPS-like (7) and PPS-like (8) NALs with start codes
    fn sample_annexb() -> Vec<u8> {
        let sps = [0x67u8, 0x42, 0x00, 0x0A];
        let pps = [0x68u8, 0xCE, 0x3C, 0x80];
        let mut out = Vec::new();
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(&sps);
        out.extend_from_slice(&[0, 0, 0, 1]);
        out.extend_from_slice(&pps);
        out.extend_from_slice(&[0, 0, 1]);
        out.extend_from_slice(&[0x65, 0x88, 0x84]); // an IDR slice
        out
    }

    #[test]
    fn splits_and_detects_keyframe() {
        let b = sample_annexb();
        let nals = split_annex_b(&b);
        assert_eq!(nals.len(), 3);
        assert_eq!(nals[0][0] & 0x1F, 7);
        assert_eq!(nals[1][0] & 0x1F, 8);
        assert!(contains_keyframe(&b));
    }

    #[test]
    fn builds_avcc_with_lengths() {
        let b = sample_annexb();
        let (sps, pps) = extract_sps_pps(&b).unwrap();
        let avcc = build_avcc(&sps, &pps);
        // profile/compat/level derived from the sample SPS: 67 42 00 0A
        assert_eq!(&avcc[..6], &[1, 0x42, 0x00, 0x0A, 0xFF, 0xE1]);
        let sps_len = u16::from_be_bytes([avcc[6], avcc[7]]) as usize;
        assert_eq!(sps_len, sps.len());
    }

    #[test]
    fn length_prefixes_correctly() {
        let nals: Vec<&[u8]> = vec![&[1, 2, 3], &[4, 5]];
        let lp = to_length_prefixed(&nals);
        assert_eq!(lp.len(), 4 + 3 + 4 + 2);
        assert_eq!(&lp[0..4], &0x0000_0003u32.to_be_bytes());
        assert_eq!(&lp[4..7], &[1, 2, 3]);
    }

    #[test]
    fn emulation_prevention_not_a_start_code() {
        // A NAL whose payload contains the byte run 00 00 03 01 (the encoder's
        // emulation-prevention for 00 00 01). Must NOT be split there.
        let mut data = vec![0, 0, 0, 1, 0x65]; // start code + IDR
        data.extend_from_slice(&[0x12, 0x00, 0x00, 0x03, 0x01, 0xAB]); // payload
        let nals = split_annex_b(&data);
        assert_eq!(nals.len(), 1);
        assert_eq!(nals[0], &[0x65, 0x12, 0x00, 0x00, 0x03, 0x01, 0xAB][..]);
    }

    #[test]
    fn trailing_start_code_is_stripped() {
        // Stream ending with a bare 00 00 01 (no NAL after) must not leak it.
        let data = [0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x0A, 0x00, 0x00, 0x01];
        let nals = split_annex_b(&data);
        assert_eq!(nals, vec![&[0x67, 0x42, 0x00, 0x0A][..]]);
    }

    #[test]
    fn length_prefixed_roundtrip() {
        let nals: Vec<&[u8]> = vec![&[1, 2, 3], &[4, 5]];
        let lp = to_length_prefixed(&nals);
        let back = from_length_prefixed(&lp).unwrap();
        assert_eq!(back, nals);
        assert_eq!(from_length_prefixed(&[]), Some(vec![]));
    }

    #[test]
    fn length_prefixed_rejects_malformed() {
        assert_eq!(from_length_prefixed(&[0, 0, 0]), None); // truncated length
        assert_eq!(from_length_prefixed(&[0, 0, 0, 9, 1, 2]), None); // overrunning length
        assert_eq!(from_length_prefixed(&[0, 0, 0, 0]), None); // zero-length NAL
    }

    #[test]
    fn annex_b_assembly_roundtrips() {
        let nals: Vec<&[u8]> = vec![&[0x67, 0x42], &[0x68, 0xCE], &[0x65, 0x88]];
        let annexb = to_annex_b(&nals);
        let back = split_annex_b(&annexb);
        assert_eq!(back, nals);
        assert!(to_annex_b(&[]).is_empty());
    }

    /// Hand-built baseline SPS: 20×15 macroblocks, no cropping → 320×240.
    fn sps_320x240() -> Vec<u8> {
        vec![0x67, 0x42, 0xC0, 0x1E, 0xF4, 0x0A, 0x0F, 0xC8]
    }

    #[test]
    fn parses_sps_dimensions() {
        assert_eq!(parse_sps_dimensions(&sps_320x240()), Some((320, 240)));
    }

    #[test]
    fn avcc_sps_extraction_roundtrips_with_build() {
        let sps = sps_320x240();
        let pps = [0x68, 0xCE, 0x3C, 0x80];
        let avcc = build_avcc(&sps, &pps);
        let (sps_back, pps_back) = extract_sps_pps_from_avcc(&avcc).unwrap();
        assert_eq!(sps_back, sps);
        assert_eq!(pps_back, pps);
        assert_eq!(parse_sps_dimensions(&sps_back), Some((320, 240)));
    }

    #[test]
    fn avcc_extraction_rejects_malformed() {
        assert_eq!(extract_sps_pps_from_avcc(&[]), None);
        assert_eq!(extract_sps_pps_from_avcc(&[2, 0, 0, 0, 0xFF, 0xE1, 0]), None);
        let sps = sps_320x240();
        let pps = [0x68, 0xCE];
        let avcc = build_avcc(&sps, &pps);
        // Truncated at every length: never a panic.
        for cut in 0..avcc.len() {
            let _ = extract_sps_pps_from_avcc(&avcc[..cut]);
        }
    }

    #[test]
    fn sps_dimensions_reject_garbage() {
        assert_eq!(parse_sps_dimensions(&[]), None);
        assert_eq!(parse_sps_dimensions(&[0x67]), None);
        // Not an SPS NAL.
        assert_eq!(parse_sps_dimensions(&[0x68, 0x42, 0xC0, 0x1E, 0xF4]), None);
        // Truncated at every length: never a panic.
        let sps = sps_320x240();
        for cut in 0..sps.len() {
            let _ = parse_sps_dimensions(&sps[..cut]);
        }
    }

    // --- property tests ---

    use proptest::prelude::*;

    /// A NAL that is safe for Annex-B round-tripping: non-empty, doesn't end in
    /// zero (splitter trims trailing padding), and contains no start-code byte
    /// pattern inside (a real encoder's emulation-prevention guarantees this).
    fn safe_nal() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), 1..64).prop_filter("NAL must round-trip through Annex-B splitting", |b| {
            !b.ends_with(&[0]) && !b.windows(3).any(|w| w == [0, 0, 1]) && !b.windows(4).any(|w| w == [0, 0, 0, 1])
        })
    }

    proptest! {
        /// The FLV wire form is lossless: any set of NALs survives the
        /// length-prefix encode/decode cycle.
        #[test]
        fn length_prefixed_roundtrip_property(nals in prop::collection::vec(safe_nal(), 0..8)) {
            let lp = to_length_prefixed(&nals.iter().map(Vec::as_slice).collect::<Vec<_>>());
            let back = from_length_prefixed(&lp).unwrap();
            prop_assert_eq!(&back, &nals);
        }

        /// Annex-B framing of safe NALs is also lossless end to end:
        /// annex-b -> split -> length-prefix -> length-prefix-decode recovers
        /// the original NAL contents.
        #[test]
        fn annexb_roundtrip_property(nals in prop::collection::vec(safe_nal(), 1..8)) {
            let mut annexb = Vec::new();
            for nal in &nals {
                annexb.extend_from_slice(&[0, 0, 0, 1]);
                annexb.extend_from_slice(nal);
            }
            let lp = annexb_to_length_prefixed(&annexb);
            let back = from_length_prefixed(&lp).unwrap();
            prop_assert_eq!(&back, &nals);
        }

        /// A malformed length-prefixed buffer must never panic; `from_length_prefixed`
        /// returns `None` or a prefix, never panics.
        #[test]
        fn length_prefixed_never_panics(data in prop::collection::vec(any::<u8>(), 0..256)) {
            let _ = from_length_prefixed(&data);
        }

        /// Arbitrary Annex-B bytes never panic the splitter.
        #[test]
        fn annexb_split_never_panics(data in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = split_annex_b(&data);
        }
    }
}
