//! HLS output — packetizes the encoded stream into CMAF-style fMP4 segments
//! and maintains the `m3u8` media playlist that players poll.
//!
//! ```text
//! MediaPacket ──▶ HlsOutput ──▶ HlsStorage ──▶ init.mp4 / segN.m4s / playlist.m3u8
//! ```
//!
//! Design choices (the "modern HLS" shape):
//! - **fMP4 segments** (not MPEG-TS): one `init.mp4` (`ftyp`+`moov`) referenced
//!   by `#EXT-X-MAP`, media segments as `styp`+`moof`/`mdat` (`EXT-X-VERSION:7`).
//! - **Sliding window**: only the newest `window_size` segments stay in the
//!   playlist; retired segment files are deleted from storage.
//! - **Keyframe-aligned cuts**: a segment closes when a video keyframe arrives
//!   *and* the segment already spans the target duration, so every segment is
//!   independently joinable (`#EXT-X-INDEPENDENT-SEGMENTS`).
//! - Timestamps ride two accurate clocks: 90 kHz for video, the sample rate
//!   for audio — no rounding drift inside a segment.
//!
//! The output is a [`crate::sink::PacketSink`]: attach it to the engine with
//! [`crate::engine::Engine::attach_output`] and it survives reconnects of the
//! publish path untouched (the playlist simply keeps growing).

pub mod mp4;

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::codecs::{aac, h264};
use crate::models::{MediaKind, MediaPacket};
use crate::sink::PacketSink;
use crate::telemetry::Level;

/// DTS may slip backwards this far (encoder jitter) before it counts as a real
/// clock jump — mirrors the FLV muxer's tolerance so both outputs agree.
const REORDER_TOLERANCE_MS: i64 = 100;

/// Video sample duration assumed for the final sample of a segment when no
/// successor reveals the true spacing (30 fps).
const DEFAULT_VIDEO_FRAME_MS: i64 = 33;

/// Samples per AAC-LC frame — the audio track's sample duration in its own
/// timescale (the track timescale *is* the sample rate).
const AAC_FRAME_SAMPLES: u32 = 1024;

/// Tuning knobs for the HLS output.
#[derive(Debug, Clone)]
pub struct HlsConfig {
    /// Nominal segment duration in seconds. A segment actually closes on the
    /// first keyframe *after* spanning this long, so real segments run
    /// slightly longer — exactly what `EXT-X-TARGETDURATION` accounts for.
    pub target_duration_secs: u32,
    /// How many recent segments the playlist keeps (its sliding window).
    /// Older segment files are removed from storage.
    pub window_size: usize,
    /// File name of the initialization segment.
    pub init_segment_name: String,
    /// Prefix for media segment files; `<prefix><sequence>.m4s`.
    pub segment_prefix: String,
    /// File name of the media playlist.
    pub playlist_name: String,
    /// Nominal audio bitrate in bits/s, recorded in the `esds` box.
    pub audio_bitrate: u32,
}

impl Default for HlsConfig {
    fn default() -> Self {
        Self {
            target_duration_secs: 4,
            window_size: 6,
            init_segment_name: "init.mp4".to_owned(),
            segment_prefix: "seg".to_owned(),
            playlist_name: "playlist.m3u8".to_owned(),
            audio_bitrate: 128_000,
        }
    }
}

/// Where HLS files land. Implementations must make `write_file` appear atomic
/// to readers (players poll the playlist while it is being rewritten).
pub trait HlsStorage: Send {
    /// Create or replace a file's full contents.
    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()>;

    /// Delete a file (retired segment). Missing files are not an error.
    fn remove_file(&mut self, name: &str) -> io::Result<()>;
}

/// Unique-suffix source for temp files, so concurrent outputs writing into the
/// same directory never race on the same `.tmp` name.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed storage rooted at one directory — the normal production
/// layout (serve the directory over HTTP; the playlist references relative
/// names only). Writes go to a temp file and are renamed into place, so a
/// polling player never observes a half-written playlist or segment.
pub struct DirStorage {
    dir: PathBuf,
    id: u64,
}

impl DirStorage {
    /// Create (if needed) the output directory and anchor storage there.
    pub fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            id: TMP_COUNTER.fetch_add(1, Ordering::Relaxed),
        })
    }
}

impl HlsStorage for DirStorage {
    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let final_path = self.dir.join(name);
        let tmp_path = self.dir.join(format!(".{name}.tmp.{}", self.id));
        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)
    }

    fn remove_file(&mut self, name: &str) -> io::Result<()> {
        match fs::remove_file(self.dir.join(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// In-memory storage for tests and embedded use.
#[derive(Default)]
pub struct MemoryStorage {
    files: std::collections::BTreeMap<String, Vec<u8>>,
}

impl MemoryStorage {
    /// Snapshot of a stored file, if present.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.files.get(name).map(Vec::as_slice)
    }

    /// Names of every stored file, sorted.
    pub fn names(&self) -> Vec<&str> {
        self.files.keys().map(String::as_str).collect()
    }
}

impl HlsStorage for MemoryStorage {
    fn write_file(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.files.insert(name.to_owned(), data.to_vec());
        Ok(())
    }

    fn remove_file(&mut self, name: &str) -> io::Result<()> {
        self.files.remove(name);
        Ok(())
    }
}

/// Decoded AAC track parameters derived from the `AudioSpecificConfig`.
#[derive(Debug, Clone)]
struct AudioParams {
    asc: Vec<u8>,
    sample_rate: u32,
    channels: u8,
}

fn parse_audio_params(asc: &[u8]) -> Option<AudioParams> {
    let parsed = aac::parse_asc(asc)?;
    let sample_rate = *aac::SAMPLE_RATES.get(parsed.sampling_frequency_index as usize)?;
    Some(AudioParams {
        asc: asc.to_vec(),
        sample_rate,
        channels: parsed.channel_config.max(1),
    })
}

/// One sample waiting to be written into the current segment.
struct PendingSample {
    payload: Vec<u8>,
    /// Normalized decode time in milliseconds.
    dts: i64,
    /// Composition offset (PTS − DTS) in milliseconds.
    cts: i64,
    /// Keyframe / sync sample.
    is_sync: bool,
}

/// Stream time normalization — the same discipline as the FLV muxer: first DTS
/// becomes 0, small backward slips clamp, jumps beyond the tolerance re-anchor.
struct Timebase {
    origin: Option<i64>,
    last_dts: i64,
}

impl Timebase {
    fn new() -> Self {
        Self {
            origin: None,
            last_dts: i64::MIN,
        }
    }

    fn normalize(&mut self, dts: i64) -> i64 {
        let origin = *self.origin.get_or_insert(dts);
        let mut ts = dts - origin;
        if ts < self.last_dts {
            let slip = self.last_dts - ts;
            if slip > REORDER_TOLERANCE_MS {
                // Capture clock jump: continue the series from the high-water
                // mark instead of emitting backward timestamps.
                self.origin = Some(dts - self.last_dts.max(0));
                ts = self.last_dts.max(0);
                crate::log_event!(Level::Warn, "hls clock rebase", "dts" => dts);
            } else {
                ts = self.last_dts;
            }
        }
        self.last_dts = ts;
        ts
    }
}

/// Conditions worth logging once (not per packet).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)] // one-shot warning flags, clearer as named fields
struct Warned {
    no_video_config: bool,
    no_audio_config: bool,
    before_first_key: bool,
    track_not_in_init: bool,
    config_changed: bool,
}

/// Segments a live stream into HLS fMP4 files. Feed it through the engine
/// ([`crate::engine::Engine::attach_output`]) or directly via [`PacketSink`].
#[allow(clippy::struct_excessive_bools)] // each flag is a distinct segmenter state
pub struct HlsOutput<S: HlsStorage> {
    cfg: HlsConfig,
    storage: S,
    timebase: Timebase,
    avcc: Option<Vec<u8>>,
    audio: Option<AudioParams>,
    init_written: bool,
    init_has_video: bool,
    init_has_audio: bool,
    /// Saw any video ever (decides whether audio waits for a keyframe start).
    saw_video: bool,
    /// The first segment is open: media flows from the first keyframe on.
    started: bool,
    video_samples: Vec<PendingSample>,
    audio_samples: Vec<PendingSample>,
    /// Normalized DTS of the current segment's first sample.
    seg_start: Option<i64>,
    /// Next free video tick (90 kHz) on the exact sample grid. Segment bases
    /// continue this grid so consecutive fragments stay gap- and overlap-free
    /// despite millisecond quantization; a real timestamp jump (packet loss,
    /// clock rebase) still wins via the ms-derived floor.
    video_next_tick: Option<u64>,
    /// Same idea for audio ticks (track timescale = sample rate).
    audio_next_tick: Option<u64>,
    sequence: u32,
    window: VecDeque<(String, f64)>,
    max_segment_secs: f64,
    finished: bool,
    warned: Warned,
}

impl<S: HlsStorage> HlsOutput<S> {
    /// Create an output writing through `storage`.
    pub fn new(cfg: HlsConfig, storage: S) -> Self {
        Self {
            cfg,
            storage,
            timebase: Timebase::new(),
            avcc: None,
            audio: None,
            init_written: false,
            init_has_video: false,
            init_has_audio: false,
            saw_video: false,
            started: false,
            video_samples: Vec::new(),
            audio_samples: Vec::new(),
            seg_start: None,
            video_next_tick: None,
            audio_next_tick: None,
            sequence: 0,
            window: VecDeque::new(),
            max_segment_secs: 0.0,
            finished: false,
            warned: Warned::default(),
        }
    }

    /// Borrow the storage (tests inspect written files).
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Recover the storage once the output is done.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Number of media segments written so far.
    pub fn segment_count(&self) -> u32 {
        self.sequence
    }

    /// Current span of the open segment in milliseconds (0 when empty).
    fn span_ms(&self) -> i64 {
        let Some(start) = self.seg_start else {
            return 0;
        };
        let last_video = self.video_samples.last().map_or(i64::MIN, |s| s.dts);
        let last_audio = self.audio_samples.last().map_or(i64::MIN, |s| s.dts);
        last_video.max(last_audio).max(start) - start
    }

    fn target_ms(&self) -> i64 {
        i64::from(self.cfg.target_duration_secs) * 1000
    }

    fn has_samples(&self) -> bool {
        !self.video_samples.is_empty() || !self.audio_samples.is_empty()
    }

    /// Milliseconds → ticks in the video timescale.
    fn video_ticks(ms: i64) -> u64 {
        (ms.max(0) as u64).saturating_mul(u64::from(mp4::VIDEO_TIMESCALE)) / 1000
    }

    /// Milliseconds → ticks in the audio track's timescale (its sample rate).
    fn audio_ticks(&self, ms: i64) -> u64 {
        let Some(audio) = &self.audio else {
            return 0;
        };
        (ms.max(0) as u64).saturating_mul(u64::from(audio.sample_rate)) / 1000
    }

    fn video_packet(&mut self, pkt: &MediaPacket) -> io::Result<()> {
        self.saw_video = true;
        // Config: explicit first, else sniffed from a keyframe's SPS/PPS.
        if self.avcc.is_none() {
            self.avcc = h264::annexb_to_avcc(&pkt.data);
        }
        if self.avcc.is_none() {
            if !self.warned.no_video_config {
                self.warned.no_video_config = true;
                crate::log_event!(Level::Warn, "hls waiting for video config");
            }
            return Ok(());
        }
        if self.init_written && !self.init_has_video {
            self.warn_track_not_in_init();
            return Ok(());
        }
        if !self.started {
            if !pkt.is_key {
                if !self.warned.before_first_key {
                    self.warned.before_first_key = true;
                    crate::log_event!(Level::Debug, "hls waiting for first keyframe");
                }
                return Ok(());
            }
            self.started = true;
        }

        let dts = self.timebase.normalize(pkt.dts);
        let cts = pkt.pts - pkt.dts;

        // Keyframe-aligned cut: close the open segment once it spans the
        // target duration and a fresh keyframe arrives.
        if pkt.is_key && self.has_samples() && self.span_ms() >= self.target_ms() {
            self.close_segment()?;
        }

        let payload = h264::annexb_to_length_prefixed(&pkt.data);
        if self.seg_start.is_none() {
            self.seg_start = Some(dts);
        }
        self.video_samples.push(PendingSample {
            payload,
            dts,
            cts,
            is_sync: pkt.is_key,
        });
        Ok(())
    }

    fn audio_packet(&mut self, pkt: &MediaPacket) -> io::Result<()> {
        // Config: explicit ASC first, else sniffed from an ADTS wrapper.
        if self.audio.is_none() {
            let asc = aac::adts_to_asc(&pkt.data);
            if let Some(asc) = asc {
                self.audio = parse_audio_params(&asc);
            }
        }
        if self.audio.is_none() {
            if !self.warned.no_audio_config {
                self.warned.no_audio_config = true;
                crate::log_event!(Level::Warn, "hls waiting for audio config");
            }
            return Ok(());
        }
        if self.init_written && !self.init_has_audio {
            self.warn_track_not_in_init();
            return Ok(());
        }
        // Streams that carry video start on a video keyframe; audio waits so
        // the first segment begins with aligned A/V. Audio-only streams start
        // on the first packet.
        if !self.started {
            if self.saw_video {
                return Ok(());
            }
            self.started = true;
        }

        let dts = self.timebase.normalize(pkt.dts);

        // Audio-only streams cut on duration alone (no keyframes exist).
        if !self.saw_video && self.has_samples() && self.span_ms() >= self.target_ms() {
            self.close_segment()?;
        }

        let payload = aac::strip_adts(&pkt.data);
        if self.seg_start.is_none() {
            self.seg_start = Some(dts);
        }
        self.audio_samples.push(PendingSample {
            payload,
            dts,
            cts: 0,
            is_sync: true,
        });
        Ok(())
    }

    fn warn_track_not_in_init(&mut self) {
        if !self.warned.track_not_in_init {
            self.warned.track_not_in_init = true;
            crate::log_event!(Level::Warn, "hls track appeared after the init segment; dropping it");
        }
    }

    /// Write `init.mp4` once, with whichever tracks are configured by now.
    fn ensure_init(&mut self) -> io::Result<()> {
        if self.init_written {
            return Ok(());
        }
        let (width, height) = self
            .avcc
            .as_deref()
            .and_then(h264::extract_sps_pps_from_avcc)
            .and_then(|(sps, _)| h264::parse_sps_dimensions(&sps))
            .unwrap_or((0, 0));
        let video = self.avcc.as_ref().map(|avcc| mp4::VideoTrack {
            avcc: avcc.clone(),
            width,
            height,
        });
        let audio = self.audio.as_ref().map(|a| mp4::AudioTrack {
            asc: a.asc.clone(),
            sample_rate: a.sample_rate,
            channels: a.channels,
            bitrate: self.cfg.audio_bitrate,
        });
        if video.is_none() && audio.is_none() {
            return Err(io::Error::other("hls: no codec config for init segment"));
        }
        let init = mp4::init_segment(video.as_ref(), audio.as_ref());
        self.storage.write_file(&self.cfg.init_segment_name, &init)?;
        self.init_written = true;
        self.init_has_video = video.is_some();
        self.init_has_audio = audio.is_some();
        Ok(())
    }

    /// Seal the open segment: build `moof`+`mdat`, store it, roll the playlist.
    fn close_segment(&mut self) -> io::Result<()> {
        if !self.has_samples() {
            return Ok(());
        }
        self.ensure_init()?;

        let mut fragments: Vec<mp4::Fragment> = Vec::new();
        let mut seg_end_ms = self.seg_start.unwrap_or(0);

        if !self.video_samples.is_empty() {
            let samples = std::mem::take(&mut self.video_samples);
            let ms_base = Self::video_ticks(samples[0].dts);
            let mut out = Vec::with_capacity(samples.len());
            for i in 0..samples.len() {
                let next = samples
                    .get(i + 1)
                    .map_or(DEFAULT_VIDEO_FRAME_MS, |s| (s.dts - samples[i].dts).max(0));
                seg_end_ms = seg_end_ms.max(samples[i].dts + next);
                out.push(mp4::FragmentSample {
                    duration: Self::video_ticks(next).min(u64::from(u32::MAX)) as u32,
                    payload: samples[i].payload.clone(),
                    flags: mp4::sample_flags(if samples[i].is_sync { 2 } else { 1 }, samples[i].is_sync),
                    composition_offset: (samples[i].cts * i64::from(mp4::VIDEO_TIMESCALE) / 1000) as i32,
                });
            }
            // Continue the exact grid; fall back to (and never undercut) the
            // ms-derived base when the grid is empty or the stream jumped.
            let base = self.video_next_tick.map_or(ms_base, |t| t.max(ms_base));
            let span: u64 = out.iter().map(|s| u64::from(s.duration)).sum();
            self.video_next_tick = Some(base.saturating_add(span));
            fragments.push(mp4::Fragment {
                track_id: mp4::VIDEO_TRACK_ID,
                base_decode_time: base,
                samples: out,
            });
        }

        if !self.audio_samples.is_empty() {
            let samples = std::mem::take(&mut self.audio_samples);
            let ms_base = self.audio_ticks(samples[0].dts);
            let frame_ms = self
                .audio
                .as_ref()
                .map_or(23, |a| i64::from(AAC_FRAME_SAMPLES) * 1000 / i64::from(a.sample_rate));
            let mut out = Vec::with_capacity(samples.len());
            for s in &samples {
                seg_end_ms = seg_end_ms.max(s.dts + frame_ms);
                out.push(mp4::FragmentSample {
                    duration: AAC_FRAME_SAMPLES,
                    payload: s.payload.clone(),
                    flags: mp4::sample_flags(2, true),
                    composition_offset: 0,
                });
            }
            let base = self.audio_next_tick.map_or(ms_base, |t| t.max(ms_base));
            let span = u64::from(AAC_FRAME_SAMPLES) * out.len() as u64;
            self.audio_next_tick = Some(base.saturating_add(span));
            fragments.push(mp4::Fragment {
                track_id: mp4::AUDIO_TRACK_ID,
                base_decode_time: base,
                samples: out,
            });
        }

        let segment = mp4::media_segment(self.sequence, &fragments);
        let name = format!("{}{}.m4s", self.cfg.segment_prefix, self.sequence);
        self.storage.write_file(&name, &segment)?;

        let duration_secs = (seg_end_ms - self.seg_start.unwrap_or(seg_end_ms)).max(0) as f64 / 1000.0;
        self.max_segment_secs = self.max_segment_secs.max(duration_secs);
        self.sequence += 1;
        self.seg_start = None;

        self.window.push_back((name, duration_secs));
        while self.window.len() > self.cfg.window_size.max(1) {
            if let Some((old, _)) = self.window.pop_front() {
                if let Err(e) = self.storage.remove_file(&old) {
                    crate::log_event!(
                        Level::Warn,
                        "hls failed to remove old segment",
                        "file" => old.as_str(),
                        "error" => e.to_string().as_str()
                    );
                }
            }
        }
        self.write_playlist(false)
    }

    /// (Re)write the media playlist over the current window.
    fn write_playlist(&mut self, finished: bool) -> io::Result<()> {
        let target = (self.cfg.target_duration_secs as f64).max(self.max_segment_secs.ceil().max(1.0)) as u32;
        let media_sequence = self.sequence.saturating_sub(self.window.len() as u32);
        let mut text = String::with_capacity(256 + self.window.len() * 32);
        text.push_str("#EXTM3U\n");
        text.push_str("#EXT-X-VERSION:7\n");
        let _ = writeln!(text, "#EXT-X-TARGETDURATION:{target}");
        let _ = writeln!(text, "#EXT-X-MEDIA-SEQUENCE:{media_sequence}");
        text.push_str("#EXT-X-INDEPENDENT-SEGMENTS\n");
        let _ = writeln!(text, "#EXT-X-MAP:URI=\"{}\"", self.cfg.init_segment_name);
        for (name, duration) in &self.window {
            let _ = write!(text, "#EXTINF:{duration:.3},\n{name}\n");
        }
        if finished {
            text.push_str("#EXT-X-ENDLIST\n");
        }
        self.storage.write_file(&self.cfg.playlist_name, text.as_bytes())
    }
}

impl<S: HlsStorage> PacketSink for HlsOutput<S> {
    fn codecs(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) {
        if let Some(v) = avcc {
            if self.init_written && self.avcc.as_deref() != Some(v) && !self.warned.config_changed {
                self.warned.config_changed = true;
                crate::log_event!(
                    Level::Warn,
                    "hls video config changed mid-stream; keeping the original init"
                );
            }
            if self.avcc.is_none() {
                self.avcc = Some(v.to_vec());
            }
        }
        if let Some(a) = asc {
            if self.audio.is_none() {
                self.audio = parse_audio_params(a);
            }
        }
    }

    fn packet(&mut self, pkt: &MediaPacket) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        match pkt.kind {
            MediaKind::Video => self.video_packet(pkt),
            MediaKind::Audio => self.audio_packet(pkt),
        }
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.close_segment()?;
        self.write_playlist(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an AVCC from the real helpers so the embedded SPS is valid.
    fn real_avcc() -> Vec<u8> {
        let sps = [0x67, 0x42, 0xC0, 0x1E, 0xF4, 0x0A, 0x0F, 0xC8];
        let pps = [0x68, 0xCE, 0x3C, 0x80];
        h264::build_avcc(&sps, &pps)
    }

    const ASC: &[u8] = &[0x0A, 0x10]; // AAC-LC 44.1 kHz stereo

    fn key(pts: i64) -> MediaPacket {
        MediaPacket::video(pts, true, vec![0, 0, 0, 1, 0x65, 0x88])
    }

    fn inter(pts: i64) -> MediaPacket {
        MediaPacket::video(pts, false, vec![0, 0, 0, 1, 0x41, 0x77])
    }

    fn audio(pts: i64) -> MediaPacket {
        // ADTS-wrapped so the ASC can be sniffed even without codecs().
        let payload: &[u8] = &[0x21, 0x00, 0x49];
        let frame_length = 7 + payload.len();
        let mut out = vec![
            0xFF,
            0xF1,
            0x50,
            0x80,
            ((frame_length >> 3) & 0xFF) as u8,
            (((frame_length & 0x07) as u8) << 5) | 0x1F,
            0xFC,
        ];
        out.extend_from_slice(payload);
        MediaPacket {
            kind: MediaKind::Audio,
            pts,
            dts: pts,
            is_key: false,
            data: out,
        }
    }

    fn output(target_secs: u32, window: usize) -> HlsOutput<MemoryStorage> {
        let cfg = HlsConfig {
            target_duration_secs: target_secs,
            window_size: window,
            ..Default::default()
        };
        let mut out = HlsOutput::new(cfg, MemoryStorage::default());
        out.codecs(Some(&real_avcc()), Some(ASC));
        out
    }

    #[test]
    fn segments_cut_on_keyframes_past_target_duration() {
        let mut out = output(1, 6);
        out.packet(&key(0)).unwrap();
        out.packet(&audio(0)).unwrap();
        out.packet(&inter(1000)).unwrap();
        out.packet(&key(2000)).unwrap(); // spans 1s → cut here
        out.packet(&inter(3000)).unwrap();
        out.packet(&key(4000)).unwrap(); // second cut
        out.finish().unwrap();

        assert_eq!(out.segment_count(), 3);
        let names = out.storage().names();
        assert!(names.contains(&"init.mp4"));
        assert!(names.contains(&"seg0.m4s"));
        assert!(names.contains(&"seg1.m4s"));
        assert!(names.contains(&"seg2.m4s"));

        let playlist = std::str::from_utf8(out.storage().get("playlist.m3u8").unwrap()).unwrap();
        assert!(playlist.starts_with("#EXTM3U\n"));
        assert!(playlist.contains("#EXT-X-VERSION:7"));
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2"));
        assert!(playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""));
        // Segment 0 spans key(0)..inter(1000) + one default frame = 1.033 s.
        assert!(playlist.contains("#EXTINF:1.033,\nseg0.m4s"));
        assert!(playlist.ends_with("#EXT-X-ENDLIST\n"));
    }

    #[test]
    fn no_cut_before_target_duration() {
        let mut out = output(4, 6);
        out.packet(&key(0)).unwrap();
        out.packet(&key(1000)).unwrap(); // only 1s in: no cut
        out.packet(&inter(1500)).unwrap();
        out.finish().unwrap();
        assert_eq!(out.segment_count(), 1);
    }

    #[test]
    fn waits_for_first_keyframe_and_configs() {
        let mut out = output(2, 6);
        out.packet(&inter(0)).unwrap(); // before any key: dropped
        out.packet(&key(100)).unwrap();
        out.finish().unwrap();
        assert_eq!(out.segment_count(), 1);
        let playlist = std::str::from_utf8(out.storage().get("playlist.m3u8").unwrap()).unwrap();
        assert!(playlist.contains("#EXTINF:0.033,\nseg0.m4s"));
    }

    #[test]
    fn sliding_window_prunes_old_segments() {
        let mut out = output(1, 2);
        // Keys 1 s apart: a segment closes on the key that finds the open
        // segment spanning >= 1 s (here: every second key).
        for g in 0..5 {
            out.packet(&key(i64::from(g) * 1000)).unwrap();
        }
        out.finish().unwrap();
        assert_eq!(out.segment_count(), 3);
        // Window keeps only the newest two, and old files are deleted.
        let names = out.storage().names();
        assert!(!names.contains(&"seg0.m4s"));
        assert!(names.contains(&"seg1.m4s"));
        assert!(names.contains(&"seg2.m4s"));
        let playlist = std::str::from_utf8(out.storage().get("playlist.m3u8").unwrap()).unwrap();
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:1"));
        assert!(!playlist.contains("seg0.m4s"));
    }

    #[test]
    fn audio_only_stream_segments_on_duration() {
        let mut out = HlsOutput::new(
            HlsConfig {
                target_duration_secs: 1,
                window_size: 8,
                ..Default::default()
            },
            MemoryStorage::default(),
        );
        out.codecs(None, Some(ASC));
        for i in 0..100 {
            out.packet(&audio(i64::from(i) * 23)).unwrap();
        }
        out.finish().unwrap();
        assert!(out.segment_count() >= 2, "audio-only cuts on duration");
        let playlist = std::str::from_utf8(out.storage().get("playlist.m3u8").unwrap()).unwrap();
        assert!(playlist.contains("#EXT-X-MAP"));
    }

    #[test]
    fn sniffed_configs_from_packets_work() {
        let mut out = HlsOutput::new(HlsConfig::default(), MemoryStorage::default());
        // Keyframe carries SPS/PPS inline; audio arrives ADTS-wrapped.
        let mut keyframe = vec![0, 0, 0, 1];
        keyframe.extend_from_slice(&[0x67, 0x42, 0xC0, 0x1E, 0xF4, 0x0A, 0x0F, 0xC8]);
        keyframe.extend_from_slice(&[0, 0, 0, 1]);
        keyframe.extend_from_slice(&[0x68, 0xCE, 0x3C, 0x80]);
        keyframe.extend_from_slice(&[0, 0, 0, 1]);
        keyframe.extend_from_slice(&[0x65, 0x88]);
        out.packet(&MediaPacket::video(0, true, keyframe)).unwrap();
        out.packet(&audio(10)).unwrap();
        out.finish().unwrap();
        assert_eq!(out.segment_count(), 1);
        let init = out.storage().get("init.mp4").unwrap();
        assert!(init.windows(4).any(|w| w == b"avc1"));
        assert!(init.windows(4).any(|w| w == b"mp4a"));
    }

    #[test]
    fn finish_is_idempotent_and_late_packets_ignored() {
        let mut out = output(2, 6);
        out.packet(&key(0)).unwrap();
        out.finish().unwrap();
        let count = out.segment_count();
        out.packet(&inter(40)).unwrap();
        out.finish().unwrap();
        assert_eq!(out.segment_count(), count);
    }

    #[test]
    fn b_frame_cts_lands_in_trun() {
        let mut out = output(2, 6);
        // B-frame: pts behind dts → negative composition offset.
        let mut pkt = key(0);
        pkt.pts = -40;
        out.packet(&pkt).unwrap();
        out.packet(&inter(40)).unwrap();
        out.finish().unwrap();
        let seg = out.storage().get("seg0.m4s").unwrap();
        // Find the trun and check the first sample's composition offset is -40ms
        // in 90 kHz ticks (-3600).
        let mut found = false;
        for w in seg.windows(4) {
            if w == b"trun" {
                found = true;
                break;
            }
        }
        assert!(found, "segment must contain a trun box");
        let expected = (-40i64 * 90_000 / 1000) as i32;
        let needle = expected.to_be_bytes();
        assert!(seg.windows(4).any(|w| w == needle), "negative cto encoded: {expected}");
    }
}
