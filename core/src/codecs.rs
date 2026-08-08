//! Small, focused codec helpers. These don't *decode* media (that's the
//! decoder's job on the viewer side) — they only extract the container bits
//! the muxer needs, and reshape packets into the form FLV wants.
//!
//! * Video: H.264. Input is **Annex B** (start-code separated NAL units, what
//!   most Android encoders emit); FLV wants **length-prefixed** NALs, plus the
//!   `AVCDecoderConfigurationRecord` (SPS/PPS) as a separate sequence header.
//! * Audio: AAC-LC. Input may be **ADTS**-wrapped (with headers) or raw; FLV
//!   wants raw frames + the 2-byte `AudioSpecificConfig`.

pub mod aac;
pub mod h264;
