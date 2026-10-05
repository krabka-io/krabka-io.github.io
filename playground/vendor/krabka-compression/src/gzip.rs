//! Gzip (RFC-1952), through the configured `flate2` backend.

use std::{
    cell::RefCell,
    io::{Read, Write},
};

use bytes::Bytes;
use flate2::{Compression as GzipLevel, Crc, read::GzDecoder, write::DeflateEncoder};

use crate::CompressionError;

// Empty final fixed-Huffman block, then zero CRC32 and input size.
const EMPTY_BODY: &[u8] = &[3, 0, 0, 0, 0, 0, 0, 0, 0, 0];

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    compress_at(data, GzipLevel::default())
}

/// Compress at `level`, which the caller has checked. -1 is the zlib default
/// level, as Java's `Deflater.DEFAULT_COMPRESSION`.
pub fn compress_with_level(data: &[u8], level: i32) -> Result<Bytes, CompressionError> {
    let level = u32::try_from(level).map_or_else(|_| GzipLevel::default(), GzipLevel::new);
    compress_at(data, level)
}

fn compress_at(data: &[u8], level: GzipLevel) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        let mut out = Vec::with_capacity(20);
        out.extend_from_slice(&frame_header(level));
        out.extend_from_slice(EMPTY_BODY);
        return Ok(Bytes::from(out));
    }
    // One encoder per thread, replaced when its level changes. Its window and
    // staging buffer have fixed sizes; completed output is taken each time.
    thread_local! {
        static ENCODER: RefCell<Option<(GzipLevel, DeflateEncoder<Vec<u8>>)>> =
            const { RefCell::new(None) };
    }
    ENCODER
        .try_with(|cached| {
            let mut cached = cached.borrow_mut();
            let result = compress_reused(data, level, &mut cached);
            if result.is_err() {
                // Do not reuse a stream that failed before its end mark.
                cached.take();
            }
            result
        })
        // A different thread-local destructor may encode after this cache drops.
        .unwrap_or_else(|_| compress_reused(data, level, &mut None))
}

fn compress_reused(
    data: &[u8],
    level: GzipLevel,
    cached: &mut Option<(GzipLevel, DeflateEncoder<Vec<u8>>)>,
) -> Result<Bytes, CompressionError> {
    // Allow for framing and stored-block overhead. Vec can grow if needed.
    let capacity = data
        .len()
        .saturating_add(data.len().div_ceil(16 * 1024).saturating_mul(5))
        .saturating_add(20);
    let out = Vec::with_capacity(capacity);
    if let Some((cached_level, encoder)) = cached.as_mut()
        && *cached_level == level
    {
        encoder.reset(out)?;
    } else {
        // Release the previous workspace before allocating a different level.
        cached.take();
        *cached = Some((level, DeflateEncoder::new(out, level)));
    }
    let encoder = &mut cached.as_mut().expect("the encoder was initialized").1;
    encoder.get_mut().extend_from_slice(&frame_header(level));
    encoder.write_all(data)?;
    encoder.try_finish()?;
    // Leave the cached encoder finished with an empty writer. Reset it only
    // when the next frame starts, so drop does not write an empty stream.
    let mut out = std::mem::take(encoder.get_mut());
    let mut crc = Crc::new();
    crc.update(data);
    out.extend_from_slice(&crc.sum().to_le_bytes());
    out.extend_from_slice(&crc.amount().to_le_bytes());
    Ok(Bytes::from(out))
}

fn frame_header(level: GzipLevel) -> [u8; 10] {
    // GzBuilder defaults: no optional fields, zero mtime, unknown OS. XFL
    // distinguishes the best and fastest levels.
    let xfl = if level.level() >= 9 {
        2
    } else if level.level() <= 1 {
        4
    } else {
        0
    };
    [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, xfl, 255]
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty gzip payload".into()));
    }
    // This complete frame is already valid, including its block, CRC and size.
    // MTIME, XFL and OS are unrestricted; optional headers use the decoder.
    if data.len() == 20 && data.starts_with(&[0x1f, 0x8b, 8, 0]) && &data[10..] == EMPTY_BODY {
        return Ok(Bytes::new());
    }
    let decoder = GzDecoder::new(data);
    // Read at most `max_output + 1` bytes: the extra byte lets us detect that
    // the real output exceeds the cap without ever materializing it.
    let mut limited = decoder.take((max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("gzip decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    if out.is_empty() {
        return Ok(Bytes::new());
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {

    use super::*;

    const HELLO: &[u8] = b"hello kafka, this is a moderately repetitive payload to compress";
    const BIG_CAP: usize = 256 * 1024 * 1024;

    #[test]
    fn roundtrip() {
        let z = compress(HELLO).unwrap();
        assert2::assert!(z.len() < HELLO.len() + 32);
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
    }

    #[test]
    fn decompress_empty_rejected() {
        assert2::assert!(matches!(
            decompress(b"", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn decompress_garbage_rejected() {
        assert2::assert!(matches!(
            decompress(b"this is not gzip", BIG_CAP),
            Err(CompressionError::InvalidData(_))
        ));
    }

    #[test]
    fn compress_empty_produces_valid_frame() {
        let z = compress(b"").unwrap();
        assert2::assert!(!z.is_empty());
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == b"");
    }

    #[test]
    fn decompression_bomb_rejected() {
        // 64 MiB of zeros compresses tiny but expands hugely.
        let bomb = vec![0u8; 64 * 1024 * 1024];
        let z = compress(&bomb).unwrap();
        assert2::assert!(matches!(
            decompress(&z, 1024),
            Err(CompressionError::TooLarge { limit: 1024 })
        ));
        let back = decompress(&z, BIG_CAP).unwrap();
        assert2::assert!(back.as_ref() == bomb.as_slice());
    }

    #[test]
    fn decompress_at_exact_cap_succeeds() {
        let z = compress(HELLO).unwrap();
        // Output of exactly `max_output` bytes is allowed (cap check is
        // `len > max_output`, not `>=`).
        let back = decompress(&z, HELLO.len()).unwrap();
        assert2::assert!(back.as_ref() == HELLO);
        // One byte under the exact size is rejected.
        assert2::assert!(matches!(
            decompress(&z, HELLO.len() - 1),
            Err(CompressionError::TooLarge { limit }) if limit == HELLO.len() - 1
        ));
    }
}
