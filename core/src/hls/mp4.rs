//! Minimal ISO BMFF (MP4) writer for CMAF-style fragmented output — exactly the
//! shape HLS wants: one **initialization segment** (`ftyp` + `moov` with empty
//! sample tables and `mvex` track extends) plus **media segments** (`styp` +
//! `moof`/`mdat` fragments).
//!
//! Only what live HLS needs is implemented: H.264 (`avc1` + `avcC`) and AAC-LC
//! (`mp4a` + `esds`), per-sample duration/size/flags/composition offsets, and
//! `default-base-is-moof` addressing. All integers are written big-endian per
//! ISO 14496-12.

/// Video timescale: 90 kHz, the conventional clock for video in MPEG systems.
pub const VIDEO_TIMESCALE: u32 = 90_000;

/// Video track id inside the init segment.
pub const VIDEO_TRACK_ID: u32 = 1;
/// Audio track id inside the init segment.
pub const AUDIO_TRACK_ID: u32 = 2;

/// Build one box: `size(4) + type(4) + payload`.
fn mp4_box(tag: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let size = 8 + payload.len();
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.extend_from_slice(&tag);
    out.extend_from_slice(payload);
    out
}

/// Build a full box: `size + type + version(1) + flags(3) + payload`.
fn full_box(tag: [u8; 4], version: u8, flags: u32, payload: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(4 + payload.len());
    p.push(version);
    p.extend_from_slice(&flags.to_be_bytes()[1..]);
    p.extend_from_slice(payload);
    mp4_box(tag, &p)
}

fn u16(v: u16, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn u32(v: u32, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn u64(v: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// The 3��3 identity transform matrix, as MP4 stores it (16.16 / 2.30 fixed point).
const UNITY_MATRIX: [u8; 36] = [
    0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, //
    0x00, 0x00, 0x00, 0x00,
];

/// Pack a three-letter language code the way `mdhd` wants it (3 × 5 bits).
fn language(code: [u8; 3]) -> u16 {
    code.iter().fold(0u16, |acc, c| (acc << 5) | u16::from(c & 0x1F))
}

/// Everything the init segment needs about the video track.
pub struct VideoTrack {
    /// `AVCDecoderConfigurationRecord` (SPS/PPS container).
    pub avcc: Vec<u8>,
    /// Encoded width in pixels.
    pub width: u32,
    /// Encoded height in pixels.
    pub height: u32,
}

/// Everything the init segment needs about the audio track.
pub struct AudioTrack {
    /// `AudioSpecificConfig` (the 2-byte AAC-LC form).
    pub asc: Vec<u8>,
    /// Sampling rate in Hz (also the track timescale).
    pub sample_rate: u32,
    /// Channel count.
    pub channels: u8,
    /// Nominal bitrate in bits/s (metadata only).
    pub bitrate: u32,
}

fn ftyp() -> Vec<u8> {
    let mut p = Vec::with_capacity(20);
    p.extend_from_slice(b"iso5"); // major brand
    u32(512, &mut p); // minor version
    for compat in [*b"iso5", *b"iso6", *b"mp41"] {
        p.extend_from_slice(&compat);
    }
    mp4_box(*b"ftyp", &p)
}

fn styp() -> Vec<u8> {
    let mut p = Vec::with_capacity(16);
    p.extend_from_slice(b"iso5");
    u32(512, &mut p);
    for compat in [*b"iso5", *b"iso6"] {
        p.extend_from_slice(&compat);
    }
    mp4_box(*b"styp", &p)
}

fn mvhd(next_track_id: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(100);
    u32(0, &mut p); // creation time
    u32(0, &mut p); // modification time
    u32(1000, &mut p); // timescale (movie-level, ms)
    u32(0, &mut p); // duration (live: unknown)
    u32(0x0001_0000, &mut p); // rate 1.0
    u16(0x0100, &mut p); // volume 1.0
    u16(0, &mut p); // reserved
    u32(0, &mut p); // reserved
    u32(0, &mut p); // reserved
    p.extend_from_slice(&UNITY_MATRIX);
    for _ in 0..6 {
        u32(0, &mut p); // pre_defined
    }
    u32(next_track_id, &mut p);
    full_box(*b"mvhd", 0, 0, &p)
}

fn tkhd(track_id: u32, width: u32, height: u32, volume: u16) -> Vec<u8> {
    let mut p = Vec::with_capacity(84);
    u32(0, &mut p); // creation time
    u32(0, &mut p); // modification time
    u32(track_id, &mut p);
    u32(0, &mut p); // reserved
    u32(0, &mut p); // duration
    u32(0, &mut p); // reserved
    u32(0, &mut p); // reserved
    u16(0, &mut p); // layer
    u16(0, &mut p); // alternate group
    u16(volume, &mut p);
    u16(0, &mut p); // reserved
    p.extend_from_slice(&UNITY_MATRIX);
    u32(width << 16, &mut p); // 16.16 fixed point
    u32(height << 16, &mut p);
    full_box(*b"tkhd", 0, 0x03, &p) // track_enabled | track_in_movie
}

fn mdhd(timescale: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(24);
    u32(0, &mut p); // creation
    u32(0, &mut p); // modification
    u32(timescale, &mut p);
    u32(0, &mut p); // duration
    u16(language(*b"und"), &mut p);
    u16(0, &mut p); // pre_defined
    full_box(*b"mdhd", 0, 0, &p)
}

fn hdlr(handler: [u8; 4]) -> Vec<u8> {
    let mut p = Vec::with_capacity(25);
    u32(0, &mut p); // pre_defined
    p.extend_from_slice(&handler);
    for _ in 0..3 {
        u32(0, &mut p); // reserved
    }
    p.push(0); // name: empty, null-terminated
    full_box(*b"hdlr", 0, 0, &p)
}

fn dref() -> Vec<u8> {
    // One entry: a self-contained `url ` (no external data reference).
    let url = full_box(*b"url ", 0, 0x01, &[]);
    let mut p = Vec::with_capacity(8 + url.len());
    u32(1, &mut p); // entry count
    p.extend_from_slice(&url);
    full_box(*b"dref", 0, 0, &p)
}

fn dinf() -> Vec<u8> {
    mp4_box(*b"dinf", &dref())
}

fn empty_table(tag: [u8; 4]) -> Vec<u8> {
    full_box(tag, 0, 0, &0u32.to_be_bytes()) // entry count = 0
}

fn stsz_empty() -> Vec<u8> {
    let mut p = Vec::with_capacity(8);
    u32(0, &mut p); // sample_size (0 = per-sample sizes)
    u32(0, &mut p); // sample count
    full_box(*b"stsz", 0, 0, &p)
}

/// `avc1` sample entry carrying the `avcC` decoder configuration.
fn avc1_entry(track: &VideoTrack) -> Vec<u8> {
    let mut p = Vec::with_capacity(86 + track.avcc.len());
    p.extend_from_slice(&[0u8; 6]); // reserved
    u16(1, &mut p); // data_reference_index
    u16(0, &mut p); // pre_defined
    u16(0, &mut p); // reserved
    for _ in 0..3 {
        u32(0, &mut p); // pre_defined
    }
    u16(track.width as u16, &mut p);
    u16(track.height as u16, &mut p);
    u32(0x0048_0000, &mut p); // horizontal resolution 72 dpi
    u32(0x0048_0000, &mut p); // vertical resolution
    u32(0, &mut p); // reserved
    u16(1, &mut p); // frame_count
    p.extend_from_slice(&[0u8; 32]); // compressor name (empty)
    u16(0x0018, &mut p); // depth
    p.extend_from_slice(&0xFFFF_u16.to_be_bytes()); // pre_defined = -1
    p.extend_from_slice(&mp4_box(*b"avcC", &track.avcc));
    mp4_box(*b"avc1", &p)
}

/// `mp4a` sample entry carrying the `esds` descriptor chain.
fn mp4a_entry(track: &AudioTrack) -> Vec<u8> {
    let mut p = Vec::with_capacity(76 + track.asc.len());
    p.extend_from_slice(&[0u8; 6]); // reserved
    u16(1, &mut p); // data_reference_index
    u32(0, &mut p); // reserved
    u32(0, &mut p); // reserved
    u16(track.channels.into(), &mut p);
    u16(16, &mut p); // sample size (bits)
    u16(0, &mut p); // pre_defined
    u16(0, &mut p); // reserved
    u32(track.sample_rate << 16, &mut p); // 16.16 fixed point
    p.extend_from_slice(&esds(track));
    mp4_box(*b"mp4a", &p)
}

/// `ES_Descriptor` → `DecoderConfigDescriptor` → `DecSpecificInfo` + `SLConfig`,
/// the MPEG-4 elementary stream description AAC needs. Uses single-byte
/// descriptor lengths (the payloads are always far below 128 bytes).
fn esds(track: &AudioTrack) -> Vec<u8> {
    // DecSpecificInfo: the raw AudioSpecificConfig.
    let mut dsi = Vec::with_capacity(2 + track.asc.len());
    dsi.push(0x05); // tag
    dsi.push(track.asc.len() as u8);
    dsi.extend_from_slice(&track.asc);

    // DecoderConfigDescriptor: AAC-LC audio stream.
    let dcd_len = 13 + dsi.len();
    let mut dcd = Vec::with_capacity(2 + dcd_len);
    dcd.push(0x04); // tag
    dcd.push(dcd_len as u8);
    dcd.push(0x40); // objectTypeIndication: ISO 14496-3 audio
    dcd.push(0x15); // streamType audio (0x05<<2 | upstream 0 | reserved 1)
    dcd.extend_from_slice(&[0, 0, 0]); // bufferSizeDB
    u32(track.bitrate, &mut dcd); // maxBitrate
    u32(track.bitrate, &mut dcd); // avgBitrate
    dcd.extend_from_slice(&dsi);

    // SLConfigDescriptor: predefined 2 (the only common value).
    let slc = [0x06u8, 0x01, 0x02];

    let es_len = 3 + dcd.len() + slc.len();
    let mut es = Vec::with_capacity(2 + es_len);
    es.push(0x03); // tag: ES_Descriptor
    es.push(es_len as u8);
    u16(1, &mut es); // ES_ID
    es.push(0); // streamDependence/URL/OCR/priority flags
    es.extend_from_slice(&dcd);
    es.extend_from_slice(&slc);

    full_box(*b"esds", 0, 0, &es)
}

fn stsd(sample_entry: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(4 + sample_entry.len());
    u32(1, &mut p); // entry count
    p.extend_from_slice(sample_entry);
    full_box(*b"stsd", 0, 0, &p)
}

fn stbl(sample_entry: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&stsd(sample_entry));
    p.extend_from_slice(&empty_table(*b"stts"));
    p.extend_from_slice(&empty_table(*b"stsc"));
    p.extend_from_slice(&stsz_empty());
    p.extend_from_slice(&empty_table(*b"stco"));
    mp4_box(*b"stbl", &p)
}

fn video_trak(track: &VideoTrack) -> Vec<u8> {
    let mut minf = Vec::new();
    minf.extend_from_slice(&full_box(*b"vmhd", 0, 1, &[0u8; 8]));
    minf.extend_from_slice(&dinf());
    minf.extend_from_slice(&stbl(&avc1_entry(track)));

    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd(VIDEO_TIMESCALE));
    mdia.extend_from_slice(&hdlr(*b"vide"));
    mdia.extend_from_slice(&mp4_box(*b"minf", &minf));

    let mut trak_bytes = Vec::new();
    trak_bytes.extend_from_slice(&tkhd(VIDEO_TRACK_ID, track.width, track.height, 0));
    trak_bytes.extend_from_slice(&mp4_box(*b"mdia", &mdia));
    mp4_box(*b"trak", &trak_bytes)
}

fn audio_trak(track: &AudioTrack) -> Vec<u8> {
    let mut minf = Vec::new();
    minf.extend_from_slice(&full_box(*b"smhd", 0, 0, &[0u8; 4]));
    minf.extend_from_slice(&dinf());
    minf.extend_from_slice(&stbl(&mp4a_entry(track)));

    let mut mdia = Vec::new();
    mdia.extend_from_slice(&mdhd(track.sample_rate));
    mdia.extend_from_slice(&hdlr(*b"soun"));
    mdia.extend_from_slice(&mp4_box(*b"minf", &minf));

    let mut trak_bytes = Vec::new();
    trak_bytes.extend_from_slice(&tkhd(AUDIO_TRACK_ID, 0, 0, 0x0100));
    trak_bytes.extend_from_slice(&mp4_box(*b"mdia", &mdia));
    mp4_box(*b"trak", &trak_bytes)
}

fn trex(track_id: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(20);
    u32(track_id, &mut p);
    u32(1, &mut p); // default_sample_description_index
    u32(0, &mut p); // default_sample_duration
    u32(0, &mut p); // default_sample_size
    u32(0, &mut p); // default_sample_flags
    full_box(*b"trex", 0, 0, &p)
}

/// Build the initialization segment (`ftyp` + `moov`). Either track may be
/// absent (audio-only / video-only streams); at least one must exist.
pub fn init_segment(video: Option<&VideoTrack>, audio: Option<&AudioTrack>) -> Vec<u8> {
    let mut next_track = 1u32;
    let mut moov = Vec::new();
    if let Some(v) = video {
        moov.extend_from_slice(&video_trak(v));
        next_track = next_track.max(VIDEO_TRACK_ID + 1);
    }
    if let Some(a) = audio {
        moov.extend_from_slice(&audio_trak(a));
        next_track = next_track.max(AUDIO_TRACK_ID + 1);
    }

    let mut mvex = Vec::new();
    if video.is_some() {
        mvex.extend_from_slice(&trex(VIDEO_TRACK_ID));
    }
    if audio.is_some() {
        mvex.extend_from_slice(&trex(AUDIO_TRACK_ID));
    }

    let mut moov_body = Vec::with_capacity(108 + moov.len() + mvex.len());
    moov_body.extend_from_slice(&mvhd(next_track));
    moov_body.extend_from_slice(&moov);
    moov_body.extend_from_slice(&mp4_box(*b"mvex", &mvex));

    let mut out = ftyp();
    out.extend_from_slice(&mp4_box(*b"moov", &moov_body));
    out
}

/// One sample inside a media fragment.
pub struct FragmentSample {
    /// Sample duration in the track's timescale.
    pub duration: u32,
    /// Compressed payload (length-prefixed NALs for video, raw AAC for audio).
    pub payload: Vec<u8>,
    /// ISO 14496-12 sample flags (dependency + sync bits).
    pub flags: u32,
    /// Composition offset (PTS − DTS) in the track's timescale; may be negative
    /// with B-frames (encoded via version-1 `trun`).
    pub composition_offset: i32,
}

/// One track's contribution to a media segment.
pub struct Fragment {
    /// Track id (must match the init segment).
    pub track_id: u32,
    /// Decode time of the first sample, in the track's timescale.
    pub base_decode_time: u64,
    /// Samples in decode order.
    pub samples: Vec<FragmentSample>,
}

/// ISO 14496-12 sample flags: `sample_depends_on` (2 = does not depend on
/// others, i.e. an intra frame) and the sync bit.
pub fn sample_flags(depends_on: u8, is_sync: bool) -> u32 {
    (u32::from(depends_on & 0x03) << 24) | (u32::from(!is_sync) << 16)
}

/// Build one media segment: `styp` + `moof` (one `traf` per fragment) + `mdat`.
/// Uses `default-base-is-moof` addressing: each `trun` data offset is relative
/// to the start of the `moof` box and points at that track's first sample
/// inside the single `mdat`.
pub fn media_segment(sequence_number: u32, fragments: &[Fragment]) -> Vec<u8> {
    // trun flags: data-offset | sample-duration | sample-size | sample-flags |
    // sample-composition-time-offsets. Version 1 makes the cto signed.
    const TRUN_FLAGS: u32 = 0x0000_0001 | 0x0000_0100 | 0x0000_0200 | 0x0000_0400 | 0x0000_0800;
    const TFHD_FLAGS: u32 = 0x02_0000; // default-base-is-moof

    let mut moof_body = Vec::new();
    let mut mfhd_payload = Vec::with_capacity(4);
    u32(sequence_number, &mut mfhd_payload);
    moof_body.extend_from_slice(&full_box(*b"mfhd", 0, 0, &mfhd_payload));

    let mut payload_sizes: Vec<usize> = Vec::new();
    for frag in fragments {
        let mut traf = Vec::new();

        let mut tfhd_payload = Vec::with_capacity(4);
        u32(frag.track_id, &mut tfhd_payload);
        traf.extend_from_slice(&full_box(*b"tfhd", 0, TFHD_FLAGS, &tfhd_payload));

        let mut tfdt_payload = Vec::with_capacity(8);
        u64(frag.base_decode_time, &mut tfdt_payload);
        traf.extend_from_slice(&full_box(*b"tfdt", 1, 0, &tfdt_payload));

        let mut trun_payload = Vec::with_capacity(8 + frag.samples.len() * 16);
        u32(frag.samples.len() as u32, &mut trun_payload);
        u32(0, &mut trun_payload); // data offset: patched below
        for s in &frag.samples {
            u32(s.duration, &mut trun_payload);
            u32(s.payload.len() as u32, &mut trun_payload);
            u32(s.flags, &mut trun_payload);
            trun_payload.extend_from_slice(&s.composition_offset.to_be_bytes());
        }
        traf.extend_from_slice(&full_box(*b"trun", 1, TRUN_FLAGS, &trun_payload));

        payload_sizes.push(frag.samples.iter().map(|s| s.payload.len()).sum());
        moof_body.extend_from_slice(&mp4_box(*b"traf", &traf));
    }

    let mut out = mp4_box(*b"moof", &moof_body);

    // Patch each trun's data offset now that the moof size is final. Offsets
    // are relative to the moof start; the mdat payload begins right after the
    // moof plus the 8-byte mdat header, and each track's samples follow the
    // previous track's back to back.
    let mdat_payload_rel = out.len() + 8; // relative to moof start
    let mut cursor = 8; // inside the moof body
    let mut frag_idx = 0usize;
    while cursor + 8 <= out.len() && frag_idx < fragments.len() {
        let size = u32::from_be_bytes([out[cursor], out[cursor + 1], out[cursor + 2], out[cursor + 3]]) as usize;
        if out[cursor + 4..cursor + 8] == *b"traf" {
            let traf_end = cursor + size;
            let mut c = cursor + 8;
            while c + 8 <= traf_end {
                let inner = u32::from_be_bytes([out[c], out[c + 1], out[c + 2], out[c + 3]]) as usize;
                if out[c + 4..c + 8] == *b"trun" {
                    // header(8) + version/flags(4) + sample_count(4) → offset field
                    let at = c + 16;
                    let data_offset = mdat_payload_rel + payload_sizes[..frag_idx].iter().sum::<usize>();
                    out[at..at + 4].copy_from_slice(&(data_offset as u32).to_be_bytes());
                    break;
                }
                c += inner.max(8);
            }
            frag_idx += 1;
        }
        cursor += size.max(8);
    }

    // mdat: header + all payloads, in fragment/sample order.
    let total_payload: usize = payload_sizes.iter().sum();
    out.extend_from_slice(&((total_payload + 8) as u32).to_be_bytes());
    out.extend_from_slice(b"mdat");
    for frag in fragments {
        for s in &frag.samples {
            out.extend_from_slice(&s.payload);
        }
    }

    let mut seg = styp();
    seg.extend_from_slice(&out);
    seg
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk top-level boxes, returning `(type, offset, size)`.
    fn boxes(bytes: &[u8]) -> Vec<([u8; 4], usize, usize)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 8 <= bytes.len() {
            let size = u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]) as usize;
            if size < 8 || pos + size > bytes.len() {
                break;
            }
            let mut tag = [0u8; 4];
            tag.copy_from_slice(&bytes[pos + 4..pos + 8]);
            out.push((tag, pos, size));
            pos += size;
        }
        out
    }

    /// Find a nested box by path, returning its payload (after the box header).
    fn find<'a>(bytes: &'a [u8], path: &[&[u8; 4]]) -> Option<&'a [u8]> {
        let mut scope = bytes;
        for name in path {
            let (_, off, size) = boxes(scope).iter().find(|(t, _, _)| t == *name).copied()?;
            scope = &scope[off + 8..off + size];
        }
        Some(scope)
    }

    fn video() -> VideoTrack {
        VideoTrack {
            avcc: vec![
                0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
            ],
            width: 320,
            height: 240,
        }
    }

    fn audio() -> AudioTrack {
        AudioTrack {
            asc: vec![0x0A, 0x10],
            sample_rate: 44_100,
            channels: 2,
            bitrate: 128_000,
        }
    }

    #[test]
    fn init_segment_has_expected_top_level_layout() {
        let init = init_segment(Some(&video()), Some(&audio()));
        let top: Vec<[u8; 4]> = boxes(&init).iter().map(|(t, _, _)| *t).collect();
        assert_eq!(top, vec![*b"ftyp", *b"moov"]);
        assert!(find(&init, &[b"moov", b"mvhd"]).is_some());
        assert!(find(&init, &[b"moov", b"mvex"]).is_some());
    }

    #[test]
    fn init_segment_carries_codec_specifics() {
        let init = init_segment(Some(&video()), Some(&audio()));
        // avcC carries the raw AVCC record; esds carries the ASC bytes.
        let v = video();
        assert!(init.windows(v.avcc.len()).any(|w| w == v.avcc.as_slice()));
        let a = audio();
        assert!(init.windows(a.asc.len()).any(|w| w == a.asc.as_slice()));
        // mvhd's next_track_id is past both tracks.
        let mvhd = find(&init, &[b"moov", b"mvhd"]).unwrap();
        assert_eq!(&mvhd[mvhd.len() - 4..], &3u32.to_be_bytes());
    }

    #[test]
    fn audio_only_init_is_well_formed() {
        let init = init_segment(None, Some(&audio()));
        let top: Vec<[u8; 4]> = boxes(&init).iter().map(|(t, _, _)| *t).collect();
        assert_eq!(top, vec![*b"ftyp", *b"moov"]);
        let mvhd = find(&init, &[b"moov", b"mvhd"]).unwrap();
        assert_eq!(&mvhd[mvhd.len() - 4..], &3u32.to_be_bytes());
    }

    #[test]
    fn media_segment_layout_and_offsets() {
        let frag = Fragment {
            track_id: VIDEO_TRACK_ID,
            base_decode_time: 0,
            samples: vec![
                FragmentSample {
                    duration: 3000,
                    payload: vec![1, 2, 3, 4],
                    flags: sample_flags(2, true),
                    composition_offset: 0,
                },
                FragmentSample {
                    duration: 3000,
                    payload: vec![5, 6],
                    flags: sample_flags(1, false),
                    composition_offset: -1500,
                },
            ],
        };
        let seg = media_segment(7, &[frag]);
        let top = boxes(&seg);
        let tags: Vec<[u8; 4]> = top.iter().map(|(t, _, _)| *t).collect();
        assert_eq!(tags, vec![*b"styp", *b"moof", *b"mdat"]);

        // mdat follows the moof immediately and holds both payloads.
        let (_, moof_off, moof_size) = top[1];
        let (_, mdat_off, mdat_size) = top[2];
        assert_eq!(mdat_off, moof_off + moof_size);
        assert_eq!(mdat_size, 8 + 6);
        assert_eq!(&seg[mdat_off + 8..mdat_off + 8 + 6], &[1, 2, 3, 4, 5, 6]);

        // mfhd carries the sequence number.
        let mfhd = find(&seg[moof_off + 8..moof_off + moof_size], &[b"mfhd"]).unwrap();
        assert_eq!(&mfhd[4..8], &7u32.to_be_bytes());
    }

    #[test]
    fn two_fragment_offsets_point_at_own_payloads() {
        let video_frag = Fragment {
            track_id: VIDEO_TRACK_ID,
            base_decode_time: 0,
            samples: vec![FragmentSample {
                duration: 3000,
                payload: vec![0xAA; 10],
                flags: sample_flags(2, true),
                composition_offset: 0,
            }],
        };
        let audio_frag = Fragment {
            track_id: AUDIO_TRACK_ID,
            base_decode_time: 0,
            samples: vec![FragmentSample {
                duration: 1024,
                payload: vec![0xBB; 4],
                flags: sample_flags(2, true),
                composition_offset: 0,
            }],
        };
        let seg = media_segment(1, &[video_frag, audio_frag]);
        let top = boxes(&seg);
        let (_, moof_off, moof_size) = top[1];
        let (_, mdat_off, _) = top[2];
        assert_eq!(mdat_off, moof_off + moof_size);

        // Locate both truns inside the moof and read their data offsets.
        let moof = &seg[moof_off + 8..moof_off + moof_size];
        let mut trun_offsets = Vec::new();
        for (tag, toff, tsize) in boxes(moof) {
            if tag != *b"traf" {
                continue;
            }
            let traf = &moof[toff + 8..toff + tsize];
            let trun = find(traf, &[b"trun"]).unwrap();
            // version/flags(4) + sample_count(4) → data offset
            trun_offsets.push(u32::from_be_bytes([trun[8], trun[9], trun[10], trun[11]]) as usize);
        }
        let payload_base = mdat_off + 8 - moof_off;
        assert_eq!(trun_offsets[0], payload_base, "video trun points at mdat payload");
        assert_eq!(trun_offsets[1], payload_base + 10, "audio trun skips the video bytes");
        assert_eq!(seg[mdat_off + 8], 0xAA);
        assert_eq!(seg[mdat_off + 8 + 10], 0xBB);
    }

    #[test]
    fn sample_flags_packing() {
        let key = sample_flags(2, true);
        assert_eq!(key & 0x0300_0000, 2 << 24);
        assert_eq!(key & 0x0001_0000, 0, "sync sample");
        let inter = sample_flags(1, false);
        assert_eq!(inter & 0x0001_0000, 0x0001_0000, "non-sync sample");
    }
}
