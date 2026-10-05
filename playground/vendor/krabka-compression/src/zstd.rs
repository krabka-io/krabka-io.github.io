//! Plain zstd frames, as Kafka writes them.
//!
//! Cluster Lab copy: upstream binds libzstd through the `zstd` crate. This
//! one uses `ruzstd`, which is pure Rust. Its encoder implements only its
//! `Fastest` level (about zstd level 1), so every level compresses at that
//! level; any zstd decoder reads the frames. The decoder reads every frame of
//! the payload, as a JVM consumer's `ZstdInputStream` does.

use std::io::Read;

use bytes::Bytes;
use ruzstd::{
    decoding::StreamingDecoder,
    encoding::{CompressionLevel, compress_to_vec},
};

use crate::CompressionError;

pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    Ok(Bytes::from(compress_to_vec(
        data,
        CompressionLevel::Fastest,
    )))
}

// ponytail: one level only; `ruzstd` leaves its higher levels unimplemented.
pub fn compress_with_level(data: &[u8], _level: i32) -> Result<Bytes, CompressionError> {
    compress(data)
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty zstd payload".into()));
    }
    let mut source = data;
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    while !source.is_empty() {
        let decoder = StreamingDecoder::new(&mut source)
            .map_err(|e| CompressionError::InvalidData(format!("zstd open: {e}")))?;
        // Read at most one byte past the budget, to detect an overflow
        // without materializing it.
        let budget = max_output.saturating_sub(out.len()) as u64 + 1;
        decoder
            .take(budget)
            .read_to_end(&mut out)
            .map_err(|e| CompressionError::InvalidData(format!("zstd decode: {e}")))?;
        if out.len() > max_output {
            return Err(CompressionError::TooLarge { limit: max_output });
        }
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_and_concatenate() {
        let data = b"hello kafka, this is a moderately repetitive payload".repeat(50);
        let z = compress(&data).unwrap();
        assert2::assert!(decompress(&z, 1 << 20).unwrap().as_ref() == data.as_slice());
        let twice = [z.as_ref(), z.as_ref()].concat();
        assert2::assert!(decompress(&twice, 1 << 20).unwrap().len() == 2 * data.len());
        assert2::assert!(matches!(
            decompress(&z, 10),
            Err(CompressionError::TooLarge { limit: 10 })
        ));
        assert2::assert!(decompress(b"not zstd", 100).is_err());
    }
}
