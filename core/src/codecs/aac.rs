//! AAC-LC helpers: parsing ADTS headers and building the FLV `AudioSpecificConfig`.

/// The ADTS header parsed just enough to rebuild `AudioSpecificConfig` and to
/// know where the raw frame begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdtsHeader {
    /// profile + 1 (1 = AAC Main, 2 = AAC LC)
    pub object_type: u8,
    /// Index into [`SAMPLE_RATES`] (e.g. 4 = 44100 Hz).
    pub sampling_frequency_index: u8,
    /// Channel configuration (1 = mono, 2 = stereo, ...).
    pub channel_config: u8,
    /// total frame length in bytes (header + payload)
    pub frame_length: usize,
    /// header size in bytes (7 or 9 if CRC)
    pub header_length: usize,
}

/// Standard AAC sampling frequency table, indexed by `sampling_frequency_index`.
pub const SAMPLE_RATES: [u32; 13] = [
    96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350,
];

/// Parse an ADTS header from the front of `data`. Returns `None` when the sync
/// word (0xFFF) or required bits aren't present.
pub fn parse_adts(data: &[u8]) -> Option<AdtsHeader> {
    if data.len() < 7 || data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return None;
    }
    let protection_absent = (data[1] & 0x01) == 1;
    let header_length = if protection_absent { 7 } else { 9 };
    let object_type = ((data[2] >> 6) & 0x03) + 1;
    let sampling_frequency_index = (data[2] >> 2) & 0x0F;
    let channel_config = ((data[2] & 0x01) << 2) | ((data[3] >> 6) & 0x03);
    let frame_length = (((data[3] & 0x03) as usize) << 11) | ((data[4] as usize) << 3) | ((data[5] >> 5) as usize);
    Some(AdtsHeader {
        object_type,
        sampling_frequency_index,
        channel_config,
        frame_length,
        header_length,
    })
}

/// Build the 2-byte `AudioSpecificConfig` FLV uses for the AAC sequence header.
///
/// Layout: [ objectTypeMinus1 (5) | samplingFreqIndex (4) | channelConfig (4) ]
pub fn build_asc(header: &AdtsHeader) -> Vec<u8> {
    let object_type_minus_1 = (header.object_type.saturating_sub(1)) & 0x1F;
    let mut asc = [0u8; 2];
    asc[0] = (object_type_minus_1 << 3) | ((header.sampling_frequency_index >> 1) & 0x07);
    asc[1] = ((header.sampling_frequency_index & 0x01) << 7) | ((header.channel_config & 0x0F) << 3);
    asc.to_vec()
}

/// Strip the ADTS header, returning just the raw AAC-LC frame.
pub fn strip_adts(data: &[u8]) -> Vec<u8> {
    match parse_adts(data) {
        Some(h) => data[h.header_length..].to_vec(),
        None => data.to_vec(), // not ADTS; return as-is
    }
}

/// One-shot helper: parse an ADTS header and build the FLV `AudioSpecificConfig`.
/// Returns `None` when the data isn't ADTS.
pub fn adts_to_asc(data: &[u8]) -> Option<Vec<u8>> {
    parse_adts(data).map(|h| build_asc(&h))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_header_and_builds_asc() {
        let data = [0xFFu8, 0xF1, 0x50, 0x80, 0x01, 0x00, 0x00, 0xDE, 0xAD];
        let h = parse_adts(&data).unwrap();
        assert_eq!(h.object_type, 2); // AAC-LC
        assert_eq!(h.sampling_frequency_index, 4);
        assert_eq!(h.channel_config, 2);
        assert_eq!(h.header_length, 7);
        let asc = build_asc(&h);
        assert_eq!(asc, [0x0A, 0x10]);
    }

    #[test]
    fn rejects_non_adts() {
        assert!(parse_adts(&[0, 1, 2, 3, 4, 5, 6]).is_none());
    }
}
