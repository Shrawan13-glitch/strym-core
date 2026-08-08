//! Transport abstraction. The muxer writes FLV bytes; this trait decides where
//! those bytes go. Today: file (tests, local dev). Tomorrow: RTMP, SRT, etc.
//!
//! Because everything upstream only depends on `Write + shutdown()`, adding a
//! new transport never touches the muxer or engine.

use std::io;

/// Anything that can receive stream bytes. Implementations may buffer and push
/// in their own pacing (a network socket, a file, an in-memory sink).
pub trait Transport: io::Write {
    /// Gracefully end the stream. For a file this is a flush; for RTMP this is
    /// the "stream ended" handshake that tells the server to close the ingest.
    fn shutdown(&mut self) -> io::Result<()>;
}

/// Writes bytes to a local file — the simplest transport, used by tests and
/// the demo to prove the pipeline produces a valid playable stream.
pub struct FileTransport<W: io::Write> {
    inner: W,
}

impl<W: io::Write> FileTransport<W> {
    /// Wrap any writer (file, socket, in-memory buffer) as a transport.
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Unwrap the underlying writer, useful after the stream ends (e.g. to
    /// read back the bytes an in-memory sink collected).
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: io::Write> io::Write for FileTransport<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: io::Write> Transport for FileTransport<W> {
    fn shutdown(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
