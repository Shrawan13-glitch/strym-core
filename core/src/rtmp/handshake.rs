//! RTMP handshake. Implements the "complex" Adobe handshake (an HMAC-SHA256
//! digest embedded in the 1536-byte C1/S1), but degrades to the simple handshake
//! when the server doesn't use digests (e.g. plain `nginx-rtmp`).
//!
//! The 1536-byte block layout:
//!   [0..4]   timestamp
//!   [4..8]   version (often zeros)
//!   [8..]    random bytes, with a 32-byte digest embedded at offset 8 or 772.

use crate::sha256::hmac_sha256;

const FMS_KEY1: &[u8] = b"Genuine Adobe Flash Player 001"; // client -> server
const FMS_KEY2: &[u8] = b"Genuine Adobe Flash Player 002"; // server -> client

const BLOCK_LEN: usize = 1536;
const DIGEST_LEN: usize = 32;
pub(crate) const DIGEST_OFFSETS: [usize; 2] = [8, 772];

/// HMAC(key, block-minus-the-digest-slot) — the value that must sit in the slot.
fn slot_digest(block: &[u8], offset: usize, key: &[u8]) -> [u8; DIGEST_LEN] {
    let mut material = Vec::with_capacity(BLOCK_LEN - DIGEST_LEN);
    material.extend_from_slice(&block[..offset]);
    material.extend_from_slice(&block[offset + DIGEST_LEN..]);
    hmac_sha256(key, &material)
}

/// True if the block already carries a valid digest at `offset` under `key`.
/// Blocks too short to hold the slot never validate (instead of panicking).
fn slot_is_valid(block: &[u8], offset: usize, key: &[u8]) -> bool {
    if block.len() < offset + DIGEST_LEN {
        return false;
    }
    block[offset..offset + DIGEST_LEN] == slot_digest(block, offset, key)
}

/// Locate the schema offset (8 or 772) in `block` whose digest validates under
/// `key`. `None` if neither does (a simple-handshake peer's block).
pub fn find_digest_offset(block: &[u8], key: &[u8]) -> Option<usize> {
    DIGEST_OFFSETS.iter().copied().find(|&o| slot_is_valid(block, o, key))
}

/// Build the client C0/C1 pair (complex form: digest embedded at offset 8).
/// Most servers accept the simpler form below; use `build_c1_simple` for those.
/// `time` doubles as a deterministic PRNG seed so tests are reproducible.
pub fn build_c1(time: u32) -> (u8, [u8; BLOCK_LEN]) {
    build_block(time, FMS_KEY1, DIGEST_OFFSETS[0])
}

/// Build the client C0/C1 pair in the classic "simple" handshake form:
/// `time + zeros + random`, with no embedded digest. This is what ffmpeg sends
/// and is accepted by nginx-rtmp, SRS (fallback) and node-media-server.
pub fn build_c1_simple(time: u32) -> (u8, [u8; BLOCK_LEN]) {
    let mut block = [0u8; BLOCK_LEN];
    fill_random(&mut block, u64::from(time));
    (3, block)
}

/// Fill the random section (`[8..]`) of a handshake block from a seed, leaving
/// the timestamp in `[..4]` and version bytes at `[4..8]`.
fn fill_random(block: &mut [u8; BLOCK_LEN], seed: u64) {
    block[..4].copy_from_slice(&(seed as u32).to_be_bytes());
    let mut x = seed;
    for b in &mut block[8..] {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        *b = (x >> 33) as u8;
    }
}

/// Build a block with a valid digest under an arbitrary key and schema offset.
/// The server side uses `FMS_KEY2` (schema 0) for its S1; clients use `FMS_KEY1`.
pub fn build_block(time: u32, key: &[u8], offset: usize) -> (u8, [u8; BLOCK_LEN]) {
    build_block_seeded(time, u64::from(time) ^ 0x2545_F491_4F6C_DD1D, key, offset)
}

/// Like [`build_block`] but with an explicit filler seed, so the server can
/// mint a fresh S1 per connection (two sessions in the same wall-clock second
/// otherwise get identical S1s).
fn build_block_seeded(time: u32, seed: u64, key: &[u8], offset: usize) -> (u8, [u8; BLOCK_LEN]) {
    let mut block = [0u8; BLOCK_LEN];
    fill_random(&mut block, u64::from(time) ^ seed ^ 0x2545_F491_4F6C_DD1D);
    let digest = slot_digest(&block, offset, key);
    block[offset..offset + DIGEST_LEN].copy_from_slice(&digest);
    (3, block)
}

/// Build a server S1 in the plain "simple" handshake form (`time + zeros +
/// random`, no embedded digest). Every RTMP client can consume this — ffmpeg
/// and our own transport in simple mode, and digest-capable clients simply fall
/// back to the simple path. Combined with [`build_s2`] echoing C1, this is the
/// maximal-compatibility server handshake (the one nginx-rtmp serves simple
/// clients).
pub fn build_s1_simple(time: u32, seed: u64) -> [u8; BLOCK_LEN] {
    let mut block = [0u8; BLOCK_LEN];
    fill_random(&mut block, seed ^ 0x2545_F491_4F6C_DD1D);
    block[..4].copy_from_slice(&time.to_be_bytes());
    block
}

/// Build a well-formed server S1 (complex form, `FMS_KEY2` digest at offset 8)
/// with a caller-chosen filler seed — per-connection freshness on the ingest
/// server.
pub fn build_s1(time: u32, seed: u64) -> [u8; BLOCK_LEN] {
    build_block_seeded(time, seed, FMS_KEY2, DIGEST_OFFSETS[0]).1
}

/// Build a well-formed server S2 echoing the client's C1. Mirrors the client's
/// [`build_c2`]: if C1 carries a valid client digest (complex handshake) that
/// digest is echoed at its offset, otherwise C1's random bytes are echoed
/// (simple handshake, what ffmpeg sends).
pub fn build_s2(c1: &[u8]) -> [u8; BLOCK_LEN] {
    let mut s2 = [0u8; BLOCK_LEN];
    if c1.len() < BLOCK_LEN {
        return s2;
    }
    s2[..8].copy_from_slice(&c1[..8]);
    if let Some(off) = find_digest_offset(c1, FMS_KEY1) {
        s2[off..off + DIGEST_LEN].copy_from_slice(&c1[off..off + DIGEST_LEN]);
    } else {
        s2[8..].copy_from_slice(&c1[8..]);
    }
    s2
}

/// Build C2 from the peer's S1 block. This adapts to either handshake style:
/// if S1 carries a valid server digest (complex handshake) we echo that digest
/// at its offset; otherwise (simple handshake) we echo S1's random bytes.
/// In both cases the peer's time/version are copied into the first 8 bytes.
/// A block shorter than the 1536-byte protocol block yields an all-zero C2
/// (the handshake will fail at the peer — never a panic here).
pub fn build_c2(s1: &[u8]) -> [u8; BLOCK_LEN] {
    let mut c2 = [0u8; BLOCK_LEN];
    if s1.len() < BLOCK_LEN {
        return c2;
    }
    c2[..8].copy_from_slice(&s1[..8]);
    if let Some(off) = find_digest_offset(s1, FMS_KEY2) {
        c2[off..off + DIGEST_LEN].copy_from_slice(&s1[off..off + DIGEST_LEN]);
    } else {
        // Simple handshake: echo the server's random block.
        c2[8..].copy_from_slice(&s1[8..]);
    }
    c2
}

/// Build a well-formed server S1 (complex form, `FMS_KEY2` digest at offset 8)
/// for tests / diagnostics.
pub fn build_s1_complex(time: u32) -> [u8; BLOCK_LEN] {
    build_s1(time, u64::from(time) ^ 0x2545_F491_4F6C_DD1D)
}

/// Does the peer's block carry a verifiable server digest? (Diagnostics/tests.)
pub fn validate_s1(s1: &[u8]) -> bool {
    find_digest_offset(s1, FMS_KEY2).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_block_digest_is_verifiable() {
        let (_c0, c1) = build_c1(12345);
        assert_eq!(find_digest_offset(&c1, FMS_KEY1), Some(8));
    }

    #[test]
    fn c2_echoes_peer() {
        let (_c0, c1) = build_c1(99);
        let c2 = build_c2(&c1);
        assert_eq!(&c2[..8], &c1[..8]);
    }

    #[test]
    fn garbage_block_has_no_s1_digest() {
        // A server that never embeds a digest reports False.
        let block = [7u8; BLOCK_LEN];
        assert!(!validate_s1(&block));
    }

    #[test]
    fn short_blocks_never_panic() {
        for len in [0usize, 1, 8, 40, 804, BLOCK_LEN - 1] {
            let block = vec![0xABu8; len];
            assert!(!validate_s1(&block));
            let c2 = build_c2(&block);
            assert_eq!(c2, [0u8; BLOCK_LEN]);
        }
    }

    #[test]
    fn simple_c1_has_no_digest() {
        let (_c0, c1) = build_c1_simple(7);
        assert_eq!(find_digest_offset(&c1, FMS_KEY1), None);
    }

    #[test]
    fn c2_echoes_simple_s1_random() {
        let (_c0, s1) = build_c1(12345); // treat as a simple-ish peer block
        let c2 = build_c2(&s1);
        // FMS_KEY2 check must fail (this block uses FMS_KEY1), so it's the simple
        // path: random bytes echoed.
        assert_eq!(&c2[8..], &s1[8..]);
        assert_eq!(&c2[..8], &s1[..8]);
    }

    #[test]
    fn c2_reflects_complex_server_digest() {
        let s1 = build_s1_complex(42);
        assert!(validate_s1(&s1));
        let c2 = build_c2(&s1);
        let off = find_digest_offset(&s1, FMS_KEY2).expect("server digest offset");
        assert_eq!(&c2[off..off + DIGEST_LEN], &s1[off..off + DIGEST_LEN]);
    }

    #[test]
    fn simple_s1_carries_no_digest_and_c2_echoes_it() {
        let s1 = build_s1_simple(100, 0x1234_5678);
        assert_eq!(find_digest_offset(&s1, FMS_KEY2), None);
        assert_eq!(u32::from_be_bytes([s1[0], s1[1], s1[2], s1[3]]), 100);
        // Two seeds mint different blocks (per-connection freshness).
        let other = build_s1_simple(100, 0x9ABC_DEF0);
        assert_ne!(&s1[8..], &other[8..]);
        // A client's C2 echoes the simple S1's random bytes.
        let c2 = build_c2(&s1);
        assert_eq!(&c2[8..], &s1[8..]);
    }
}
