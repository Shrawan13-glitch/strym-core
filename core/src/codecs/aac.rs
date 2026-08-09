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
/// word (0xFFF) or required bits aren't present, or when `data` is shorter than
/// the header itself (7 bytes without CRC, 9 with).
pub fn parse_adts(data: &[u8]) -> Option<AdtsHeader> {
    if data.len() < 7 || data[0] != 0xFF || (data[1] & 0xF0) != 0xF0 {
        return None;
    }
    let protection_absent = (data[1] & 0x01) == 1;
    let header_length = if protection_absent { 7 } else { 9 };
    if data.len() < header_length {
        return None;
    }
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

/// The 2-byte `AudioSpecificConfig` decoded back into its fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Asc {
    /// AAC object type (1 = Main, 2 = LC, ...), as carried by the ADTS header.
    pub object_type: u8,
    /// Index into [`SAMPLE_RATES`].
    pub sampling_frequency_index: u8,
    /// Channel configuration.
    pub channel_config: u8,
}

/// Parse a 2-byte `AudioSpecificConfig` (the FLV AAC sequence-header payload).
/// Returns `None` when the buffer is too short.
pub fn parse_asc(data: &[u8]) -> Option<Asc> {
    if data.len() < 2 {
        return None;
    }
    Some(Asc {
        object_type: ((data[0] >> 3) & 0x1F) + 1,
        sampling_frequency_index: ((data[0] & 0x07) << 1) | (data[1] >> 7),
        channel_config: (data[1] >> 3) & 0x0F,
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

    #[test]
    fn rejects_adts_shorter_than_its_own_header() {
        // Sync word says "CRC present" (9-byte header) but only 8 bytes exist.
        assert!(parse_adts(&[0xFF, 0xF0, 0x50, 0x80, 0x01, 0x00, 0x00, 0xDE]).is_none());
    }

    #[test]
    fn asc_roundtrip() {
        let data = [0xFFu8, 0xF1, 0x50, 0x80, 0x01, 0x00, 0x00, 0xDE, 0xAD];
        let h = parse_adts(&data).unwrap();
        let asc = build_asc(&h);
        let back = parse_asc(&asc).unwrap();
        assert_eq!(back.object_type, h.object_type);
        assert_eq!(back.sampling_frequency_index, h.sampling_frequency_index);
        assert_eq!(back.channel_config, h.channel_config);
        assert!(parse_asc(&[0x00]).is_none());
    }

    /// Build a complete ADTS frame from a parsed header so the bytes on the wire
    /// carry exactly the fields `parse_adts` reads back.
    fn adts_frame(h: &AdtsHeader) -> Vec<u8> {
        let mut out = Vec::with_capacity(h.frame_length);
        out.push(0xFF);
        // sync low nibble, MPEG-4 (ID=0), layer 0, protection absent.
        out.push(0xF1);
        // profile(2) | sf_index(4) | private(1) | channel_config(3) top bit
        out.push(
            ((h.object_type.saturating_sub(1) & 0x03) << 6)
                | ((h.sampling_frequency_index & 0x0F) << 2)
                | ((h.channel_config >> 2) & 0x01),
        );
        // channel_config low 2 bits | frame_length high 2 bits
        out.push(((h.channel_config & 0x03) << 6) | (((h.frame_length >> 11) as u8) & 0x03));
        out.push((h.frame_length >> 3) as u8);
        out.push((((h.frame_length as u8) & 0x07) << 5) | 0x1F); // buffer fullness high
        out.push(0xFC); // buffer fullness low + 1 raw data block
                        // Pad to frame_length with plausible AAC payload bytes.
        while out.len() < h.frame_length {
            out.push(0x21);
        }
        out
    }

    use proptest::prelude::*;

    fn adts_header() -> impl Strategy<Value = AdtsHeader> {
        (1u8..=4, 0u8..13, 1u8..=2, 16usize..2048).prop_map(
            |(object_type, sampling_frequency_index, channel_config, payload)| AdtsHeader {
                object_type,
                sampling_frequency_index,
                channel_config,
                header_length: 7,
                frame_length: 7 + payload,
            },
        )
    }

    proptest! {
        /// Any valid ADTS header round-trips through parse_adts -> build_asc ->
        /// parse_asc without losing a field.
        #[test]
        fn adts_to_asc_roundtrip_property(h in adts_header()) {
            let frame = adts_frame(&h);
            let parsed = parse_adts(&frame).expect("constructed frame must parse");
            prop_assert_eq!(parsed.object_type, h.object_type);
            prop_assert_eq!(parsed.sampling_frequency_index, h.sampling_frequency_index);
            prop_assert_eq!(parsed.channel_config, h.channel_config);
            prop_assert_eq!(parsed.frame_length, h.frame_length);

            let asc = build_asc(&parsed);
            let back = parse_asc(&asc).expect("ASC must parse");
            prop_assert_eq!(back.object_type, h.object_type);
            prop_assert_eq!(back.sampling_frequency_index, h.sampling_frequency_index);
            prop_assert_eq!(back.channel_config, h.channel_config);
        }

        /// Arbitrary bytes never panic either parser.
        #[test]
        fn adts_parsers_never_panic(data in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = parse_adts(&data);
            let _ = parse_asc(&data);
        }
    }
}
