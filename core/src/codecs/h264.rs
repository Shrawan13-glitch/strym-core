//! H.264 helpers: splitting Annex-B streams, building the AVCC record, and
//! converting NAL units to the length-prefixed form FLV requires.

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

/// One-shot helper: extract SPS/PPS from an Annex-B packet and build the AVCC
/// record. Returns `None` when the packet lacks SPS or PPS.
pub fn annexb_to_avcc(data: &[u8]) -> Option<Vec<u8>> {
    extract_sps_pps(data).map(|(sps, pps)| build_avcc(&sps, &pps))
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
}
