//! Packet-level outputs — the fan-out side of the pipeline.
//!
//! The engine's primary path muxes packets into FLV for the attached transport
//! (the publish path). Some destinations want *packets*, not FLV bytes:
//!
//! - a **recording** must stay one contiguous file even when the publish
//!   connection drops and the primary muxer re-emits FLV headers mid-stream;
//! - an **HLS segmenter** re-packages into fMP4 segments, a different container
//!   altogether.
//!
//! Both are [`PacketSink`]s attached via [`crate::engine::Engine::attach_output`].
//! The engine feeds them every packet that made it to the wire (so a failed
//! batch that goes back to the buffer is never double-fed) plus codec configs
//! whenever they change. A sink that errors is retired with a log, never taking
//! the primary stream down with it.

use std::io;

use crate::models::{MediaPacket, StreamConfig};
use crate::mux::{FlvMuxer, MuxError};
use crate::telemetry::Level;
use crate::transport::Transport;

/// A destination for media packets leaving the engine buffer.
///
/// Implementations must tolerate being driven from the engine thread only;
/// all methods are called sequentially, never concurrently.
pub trait PacketSink: Send {
    /// Codec configuration update. Called whenever the engine's AVCC (H.264
    /// decoder configuration record) or ASC (AAC `AudioSpecificConfig`)
    /// changes, and always before the first packet that needs it. Either
    /// argument may be `None` when that track is absent.
    fn codecs(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>);

    /// Consume one packet. Packets arrive in mux order with normalized,
    /// monotonic timestamps exactly as the primary transport received them.
    /// Returning `Err` retires this sink (it will receive no further calls).
    fn packet(&mut self, pkt: &MediaPacket) -> io::Result<()>;

    /// The stream finished ([`crate::engine::Engine::finish`]): flush and
    /// finalize any container state (close the recording, emit the final
    /// playlist with its end marker).
    fn finish(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Records the stream to FLV while live publishing continues on the primary
/// transport — the classic "record the broadcast" feature.
///
/// Unlike a byte-level tee of the FLV stream, this recorder owns its own muxer
/// with its own timebase, so reconnects on the publish path (which re-emit FLV
/// headers and may cut back to a keyframe) never corrupt the recording: the
/// file gets exactly one header, one metadata tag, and a continuous monotonic
/// timestamp series for the whole session.
pub struct RecordingOutput<W: Transport> {
    muxer: FlvMuxer<W>,
    metadata: StreamConfig,
    metadata_written: bool,
    finished: bool,
}

impl<W: Transport> RecordingOutput<W> {
    /// Start recording into `transport` (a file, typically). `metadata` feeds
    /// the recording's `onMetaData` tag.
    pub fn new(transport: W, metadata: StreamConfig) -> Self {
        Self {
            muxer: FlvMuxer::new(transport),
            metadata,
            metadata_written: false,
            finished: false,
        }
    }

    /// Recover the transport after the recording ends.
    pub fn into_inner(self) -> W {
        self.muxer.into_inner()
    }

    /// Bytes written to the recording so far (FLV header + tags).
    pub fn bytes_written(&self) -> u64 {
        self.muxer.bytes_written()
    }

    /// The `onMetaData` tag precedes everything else, sequence headers
    /// included — write it exactly once, as early as anything needs the file.
    fn ensure_metadata(&mut self) -> io::Result<()> {
        if !self.metadata_written {
            self.muxer
                .write_metadata(&self.metadata)
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.metadata_written = true;
        }
        Ok(())
    }
}

impl<W: Transport + Send> PacketSink for RecordingOutput<W> {
    fn codecs(&mut self, avcc: Option<&[u8]>, asc: Option<&[u8]>) {
        if self.finished {
            return;
        }
        // Metadata before headers, headers before media; the muxer keeps the
        // ordering and both calls are idempotent.
        let outcome = self.ensure_metadata().and_then(|()| {
            self.muxer
                .init_codecs(avcc, asc)
                .map_err(|e| io::Error::other(e.to_string()))
        });
        if let Err(e) = outcome {
            crate::log_event!(Level::Warn, "recorder codec init failed", "error" => e.to_string().as_str());
        }
    }

    fn packet(&mut self, pkt: &MediaPacket) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.ensure_metadata()?;
        let result = match self.muxer.write_packet(pkt) {
            Ok(()) => Ok(()),
            // Mirror the engine's escape hatch: a capture-clock jump re-anchors
            // the recording instead of killing it.
            Err(MuxError::Ordering(_)) => {
                self.muxer.rebase(pkt.dts);
                self.muxer.write_packet(pkt)
            }
            Err(e) => Err(e),
        };
        result.map_err(|e| match e {
            MuxError::Io(io_err) => io_err,
            other => io::Error::other(other.to_string()),
        })
    }

    fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.muxer.finish().map_err(|e| io::Error::other(e.to_string()))?;
        self.muxer.sink_mut().shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::MediaKind;
    use crate::transport::FileTransport;

    const AVCC: &[u8] = &[
        0x01, 0x42, 0x00, 0x1F, 0xFF, 0xE1, 0x00, 0x03, 0x67, 0x42, 0x00, 0x0A, 0x01, 0x00, 0x03, 0x68, 0xCE,
    ];
    const ASC: &[u8] = &[0x0A, 0x10];

    fn key(pts: i64) -> MediaPacket {
        MediaPacket::video(pts, true, vec![0, 0, 0, 1, 0x65, 0x88])
    }

    fn inter(pts: i64) -> MediaPacket {
        MediaPacket::video(pts, false, vec![0, 0, 0, 1, 0x41, 0x77])
    }

    fn audio(pts: i64) -> MediaPacket {
        MediaPacket {
            kind: MediaKind::Audio,
            pts,
            dts: pts,
            is_key: false,
            data: vec![0x21, 0x00, 0x49],
        }
    }

    /// Parse FLV bytes into `(tag_type, timestamp, body)` triples.
    fn parse_flv(bytes: &[u8]) -> Vec<(u8, u32, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pos = 13;
        while pos + 11 <= bytes.len() {
            let kind = bytes[pos];
            let size = ((bytes[pos + 1] as usize) << 16) | ((bytes[pos + 2] as usize) << 8) | bytes[pos + 3] as usize;
            let ts = ((bytes[pos + 7] as u32) << 24)
                | ((bytes[pos + 4] as u32) << 16)
                | ((bytes[pos + 5] as u32) << 8)
                | bytes[pos + 6] as u32;
            let start = pos + 11;
            if start + size + 4 > bytes.len() {
                break;
            }
            out.push((kind, ts, bytes[start..start + size].to_vec()));
            pos = start + size + 4;
        }
        out
    }

    #[test]
    fn records_one_contiguous_stream() {
        let mut rec = RecordingOutput::new(FileTransport::new(Vec::new()), StreamConfig::default());
        rec.codecs(Some(AVCC), Some(ASC));
        rec.packet(&key(0)).unwrap();
        rec.packet(&audio(10)).unwrap();
        rec.packet(&inter(40)).unwrap();
        rec.packet(&key(2000)).unwrap();
        rec.finish().unwrap();

        let bytes = rec.into_inner().into_inner();
        assert_eq!(&bytes[..3], b"FLV");
        let tags = parse_flv(&bytes);
        let kinds: Vec<u8> = tags.iter().map(|t| t.0).collect();
        // metadata, video seq, audio seq, key, audio, inter, key
        assert_eq!(kinds, vec![18, 9, 8, 9, 8, 9, 9]);
        let ts: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts:?}");
    }

    #[test]
    fn recording_survives_a_simulated_reconnect_gap() {
        // The publish path would detach/re-attach here and re-emit headers;
        // the recorder simply keeps consuming packets — one header, one series.
        let mut rec = RecordingOutput::new(FileTransport::new(Vec::new()), StreamConfig::default());
        rec.codecs(Some(AVCC), Some(ASC));
        rec.packet(&key(0)).unwrap();
        rec.packet(&inter(40)).unwrap();
        // ...connection drops; the engine cuts to the next keyframe...
        rec.packet(&key(5000)).unwrap();
        rec.packet(&inter(5040)).unwrap();
        rec.finish().unwrap();

        let bytes = rec.into_inner().into_inner();
        let tags = parse_flv(&bytes);
        let headers = tags.iter().filter(|t| t.0 == 9 && t.2.get(1) == Some(&0)).count();
        assert_eq!(headers, 1, "exactly one video sequence header in the whole file");
        let flv_headers = bytes.windows(3).filter(|w| *w == b"FLV").count();
        assert_eq!(flv_headers, 1, "exactly one FLV file header");
    }

    #[test]
    fn clock_jump_rebases_instead_of_failing() {
        let mut rec = RecordingOutput::new(FileTransport::new(Vec::new()), StreamConfig::default());
        rec.codecs(Some(AVCC), Some(ASC));
        rec.packet(&key(10_000)).unwrap();
        rec.packet(&inter(10_040)).unwrap();
        // Capture clock resets far backwards: the recorder re-anchors.
        rec.packet(&key(40)).unwrap();
        rec.finish().unwrap();
        let bytes = rec.into_inner().into_inner();
        let tags = parse_flv(&bytes);
        let ts: Vec<u32> = tags.iter().map(|t| t.1).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "monotonic: {ts:?}");
    }

    #[test]
    fn finished_recorder_ignores_late_packets() {
        let mut rec = RecordingOutput::new(FileTransport::new(Vec::new()), StreamConfig::default());
        rec.codecs(Some(AVCC), Some(ASC));
        rec.packet(&key(0)).unwrap();
        rec.finish().unwrap();
        let before = rec.bytes_written();
        rec.packet(&inter(40)).unwrap();
        rec.codecs(Some(AVCC), None);
        rec.finish().unwrap();
        assert_eq!(rec.bytes_written(), before);
    }

    #[test]
    fn sniffs_configs_from_packets_when_none_provided() {
        // No codecs() call: the first keyframe carries SPS/PPS inline, the first
        // audio packet is ADTS-wrapped — the muxer derives both configs itself.
        let mut rec = RecordingOutput::new(FileTransport::new(Vec::new()), StreamConfig::default());
        let mut keyframe = vec![0, 0, 0, 1];
        keyframe.extend_from_slice(&[0x67, 0x42, 0x00, 0x0A]);
        keyframe.extend_from_slice(&[0, 0, 0, 1]);
        keyframe.extend_from_slice(&[0x68, 0xCE]);
        keyframe.extend_from_slice(&[0, 0, 0, 1]);
        keyframe.extend_from_slice(&[0x65, 0x88]);
        rec.packet(&MediaPacket::video(0, true, keyframe)).unwrap();
        // ADTS-wrapped audio (AAC-LC, 44.1 kHz, stereo) so the ASC is derivable.
        rec.packet(&audio_adts(10)).unwrap();
        rec.finish().unwrap();

        let bytes = rec.into_inner().into_inner();
        let tags = parse_flv(&bytes);
        let seq = tags
            .iter()
            .filter(|t| matches!(t.0, 8 | 9) && t.2.get(1) == Some(&0))
            .count();
        assert_eq!(seq, 2, "video + audio sequence headers were derived");
    }

    /// An ADTS-wrapped AAC packet, mirroring what a real encoder emits.
    fn audio_adts(pts: i64) -> MediaPacket {
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
}
