//! LZ4 frame format (LZ4F), independent blocks.
//!
//! Kafka writes LZ4 in the frame format, with magic `0x04 22 4D 18`, and makes
//! these choices: 64 KiB block size, independent blocks, no block checksum, and
//! no content-size in the header. We match those defaults, so our bytes agree
//! with the output of `KafkaLZ4BlockOutputStream` for differential testing.
//!
//! Cluster Lab copy: upstream compresses every level but Kafka's default (9)
//! with LZ4 HC through `lzzzz`, a binding to the C `liblz4`. `lz4rip` is pure
//! Rust and has only the fast compressor, so here every level uses it.

use std::{cell::RefCell, io::Read};

use bytes::Bytes;
use lz4rip::frame::FrameDecoder;

use crate::CompressionError;

/// Kafka's independent-block max size (`Lz4BlockOutputStream` uses the LZ4F
/// default of 64 KiB), matched here so HC blocks line up with the fast path.
const BLOCK_SIZE: usize = 64 * 1024;

/// lz4rip emits only literals below 13 bytes, which cannot shrink a block.
const MIN_COMPRESSIBLE_BLOCK: usize = 13;

/// The high bit of a block's 4-byte little-endian size that marks it as
/// stored uncompressed, per the LZ4 frame format.
const BLOCK_UNCOMPRESSED_BIT: u32 = 0x8000_0000;

/// Independent 64 KiB blocks, without block/content checksums, content size or
/// dictionary. The final byte is the descriptor's header checksum.
const FRAME_HEADER: &[u8] = &[0x04, 0x22, 0x4D, 0x18, 0x60, 0x40, 0x82];

// Compress borrowed blocks into the result without a streaming source buffer
// and separate compressed-block buffer.
pub fn compress(data: &[u8]) -> Result<Bytes, CompressionError> {
    // Include framing and one block's compression slack so the scratch slice
    // does not double the result allocation for a nearly full block.
    let largest_block = data.len().min(BLOCK_SIZE);
    let slack = if largest_block < MIN_COMPRESSIBLE_BLOCK {
        0
    } else {
        lz4rip::block::get_maximum_output_size(largest_block) - largest_block
    };
    let capacity = data
        .len()
        .saturating_add(data.len().div_ceil(BLOCK_SIZE).saturating_mul(4))
        .saturating_add(FRAME_HEADER.len() + 4 + slack);
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(FRAME_HEADER);
    for block in data.chunks(BLOCK_SIZE) {
        if block.len() < MIN_COMPRESSIBLE_BLOCK {
            let size = u32::try_from(block.len()).expect("a short block's length fits u32")
                | BLOCK_UNCOMPRESSED_BIT;
            out.extend_from_slice(&size.to_le_bytes());
            out.extend_from_slice(block);
            continue;
        }
        let header = out.len();
        out.extend_from_slice(&0u32.to_le_bytes());
        let start = out.len();
        out.resize(
            start + lz4rip::block::get_maximum_output_size(block.len()),
            0,
        );
        // At 65,535 bytes the stateless encoder switches to the u32 table.
        // Reuse that same table; CompressorRef clears it for these large inputs.
        let compressed = if block.len() >= usize::from(u16::MAX) {
            thread_local! {
                static COMPRESSOR: RefCell<lz4rip::block::CompressorRef> =
                    RefCell::new(lz4rip::block::CompressorRef::new());
            }
            COMPRESSOR.with(|compressor| {
                compressor
                    .borrow_mut()
                    .compress_into(block, &mut out[start..])
            })
        } else {
            lz4rip::block::compress_into(block, &mut out[start..])
        };
        let n =
            compressed.map_err(|e| CompressionError::InvalidData(format!("lz4 compress: {e}")))?;
        let size = if n < block.len() {
            out.truncate(start + n);
            u32::try_from(n).expect("a 64 KiB block's compressed length fits u32")
        } else {
            out.truncate(start + block.len());
            out[start..].copy_from_slice(block);
            u32::try_from(block.len()).expect("a 64 KiB block's length fits u32")
                | BLOCK_UNCOMPRESSED_BIT
        };
        out[header..start].copy_from_slice(&size.to_le_bytes());
    }
    out.extend_from_slice(&0u32.to_le_bytes());
    Ok(Bytes::from(out))
}

/// Compress at `level`. Every level gives the bytes of [`compress`].
// ponytail: no LZ4 HC without the C library; the lab never sets a level.
pub fn compress_with_level(data: &[u8], _level: i32) -> Result<Bytes, CompressionError> {
    compress(data)
}

pub fn decompress(data: &[u8], max_output: usize) -> Result<Bytes, CompressionError> {
    if data.is_empty() {
        return Err(CompressionError::InvalidData("empty lz4 payload".into()));
    }
    let decoder = FrameDecoder::new(data);
    // Read at most `max_output + 1` bytes so we can detect overflow without
    // materializing the oversized output.
    let mut limited = decoder.take((max_output as u64).saturating_add(1));
    let mut out = Vec::with_capacity(data.len().saturating_mul(2).min(max_output));
    limited
        .read_to_end(&mut out)
        .map_err(|e| CompressionError::InvalidData(format!("lz4 decode: {e}")))?;
    if out.len() > max_output {
        return Err(CompressionError::TooLarge { limit: max_output });
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_across_blocks() {
        for data in [b"hello kafka".repeat(3), vec![0xAB; 128 * 1024]] {
            let z = compress(&data).unwrap();
            assert2::assert!(z[..4] == [0x04, 0x22, 0x4D, 0x18]);
            assert2::assert!(decompress(&z, 1 << 20).unwrap().as_ref() == data.as_slice());
        }
        assert2::assert!(matches!(
            decompress(&compress(&[0; 4096]).unwrap(), 10),
            Err(CompressionError::TooLarge { limit: 10 })
        ));
    }
}
