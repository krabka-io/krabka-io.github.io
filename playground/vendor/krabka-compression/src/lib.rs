//! Kafka wire-protocol compression codecs.
//!
//! Kafka uses four codecs on the wire: gzip, snappy, lz4, and zstd. Each codec
//! has specific framing conventions.
//!
//! - **gzip**: standard RFC-1952 gzip through `flate2`, which uses the
//!   pure-Rust `miniz_oxide` backend.
//! - **snappy**: xerial-snappy framing over `snap` raw blocks. Kafka does not
//!   use the standard Google Snappy stream format. It uses the xerial framing:
//!   an 8-byte magic header, two 4-byte version fields, then a sequence of
//!   `u32-BE` length-prefixed raw snappy chunks.
//! - **lz4**: LZ4 frame format with magic `0x04 22 4D 18`, independent blocks,
//!   and 64 KiB block size. These are the defaults of
//!   `KafkaLZ4BlockOutputStream`.
//! - **zstd**: plain zstd at compression level 3, which is Kafka's default.
//!
//! Each codec is behind a Cargo feature: `gzip`, `snappy`, `lz4`, and `zstd`.
//! All of them are enabled by default. If you disable a feature, the API stays
//! the same, but the call returns
//! `Err(`[`CompressionError::FeatureDisabled`]`)` at runtime.
//!
//! ## Compress and decompress a record payload
//!
//! ```rust
//! use krabka_compression::{CompressionType, compress, decompress};
//! use krabka_units::kibibytes;
//!
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let compressed = compress(CompressionType::Lz4, b"order-created")?;
//! let plain = decompress(CompressionType::Lz4, &compressed, kibibytes(1))?;
//! assert_eq!(plain.as_ref(), b"order-created");
//! # Ok(())
//! # }
//! ```

mod codec_type;
mod error;

use bytes::Bytes;
pub use codec_type::CompressionType;
pub use error::CompressionError;
use krabka_units::{
    ByteSize, Ratio,
    convert::{ByteSizeExt as _, RatioExt as _},
    fraction, gibibytes, mebibytes,
};
use refined_type::rule::MinMaxU64;

/// Fixed security ceiling for record decompression expansion.
pub const RECORD_DECOMPRESSION_HARD_MAX_RATIO: Ratio = fraction(100.0);

/// Fixed security ceiling for a decompressed record payload.
pub const RECORD_DECOMPRESSION_HARD_MAX_OUTPUT: ByteSize = gibibytes(1);

const RECORD_DECOMPRESSION_HARD_MAX_OUTPUT_BYTES: u64 = 1_073_741_824;
type RefinedRecordDecompressionBytes = MinMaxU64<1, RECORD_DECOMPRESSION_HARD_MAX_OUTPUT_BYTES>;

/// Validated limits for decompressing untrusted Kafka record payloads.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordDecompressionPolicy {
    max_ratio: Ratio,
    output_floor: ByteSize,
    output_ceiling: ByteSize,
}

impl RecordDecompressionPolicy {
    /// Validate policy values against the fixed decompression security bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-positive or non-finite ratio, fractional or
    /// non-positive byte limits, an inverted range, or a weakened hard bound.
    pub fn new(
        max_ratio: Ratio,
        output_floor: ByteSize,
        output_ceiling: ByteSize,
    ) -> Result<Self, String> {
        let ratio = max_ratio.as_f64();
        if !ratio.is_finite()
            || ratio <= 0.0
            || ratio > RECORD_DECOMPRESSION_HARD_MAX_RATIO.as_f64()
        {
            return Err(
                "record decompression ratio must be finite and within 0 < ratio <= 100".into(),
            );
        }
        let floor = validated_whole_bytes("record decompression output floor", output_floor)?;
        let ceiling = validated_whole_bytes("record decompression output ceiling", output_ceiling)?;
        if floor > ceiling {
            return Err("record decompression output floor exceeds ceiling".into());
        }
        Ok(Self {
            max_ratio,
            output_floor,
            output_ceiling,
        })
    }

    /// Return the maximum decompression expansion ratio.
    #[must_use]
    pub fn max_ratio(self) -> Ratio {
        self.max_ratio
    }

    /// Return the minimum decompression output budget.
    #[must_use]
    pub fn output_floor(self) -> ByteSize {
        self.output_floor
    }

    /// Return the maximum decompression output budget.
    #[must_use]
    pub fn output_ceiling(self) -> ByteSize {
        self.output_ceiling
    }

    /// Calculate the output budget for a compressed payload.
    #[must_use]
    pub fn output_limit(self, compressed: ByteSize) -> ByteSize {
        ByteSize::from_bytes_f64(
            (compressed.bytes_f64() * self.max_ratio.as_f64())
                .max(self.output_floor.bytes_f64())
                .min(self.output_ceiling.bytes_f64()),
        )
    }
}

impl Default for RecordDecompressionPolicy {
    fn default() -> Self {
        Self {
            max_ratio: RECORD_DECOMPRESSION_HARD_MAX_RATIO,
            output_floor: mebibytes(16),
            output_ceiling: RECORD_DECOMPRESSION_HARD_MAX_OUTPUT,
        }
    }
}

fn validated_whole_bytes(name: &str, value: ByteSize) -> Result<u64, String> {
    let raw = value.bytes_f64();
    if !raw.is_finite() || raw.fract() != 0.0 {
        return Err(format!("{name} must be a positive whole number of bytes"));
    }
    RefinedRecordDecompressionBytes::new(value.bytes_u64())
        .map(refined_type::Refined::into_value)
        .map_err(|error| format!("{name}: {error}"))
}

/// Compress `data` using the codec identified by `ct`.
///
/// For `CompressionType::None`, the function returns the input unchanged in a
/// new `Bytes`. For the other codecs, it dispatches to the per-codec module. If
/// the codec's Cargo feature is not enabled, it returns
/// `Err(CompressionError::FeatureDisabled(_))`.
/// # Errors
/// Returns an error when input is malformed, compression or decompression fails, or runtime rate-limit state cannot be updated.
pub fn compress(ct: CompressionType, data: &[u8]) -> Result<Bytes, CompressionError> {
    match ct {
        CompressionType::None => Ok(Bytes::copy_from_slice(data)),
        CompressionType::Gzip => gzip_compress(data),
        CompressionType::Snappy => snappy_compress(data),
        CompressionType::Lz4 => lz4_compress(data),
        CompressionType::Zstd => zstd_compress(data),
    }
}

/// Compress `data` using the codec identified by `ct` at compression `level`
/// (KIP-390).
///
/// The level must pass [`CompressionType::check_level`], which follows Kafka's
/// `CompressionType.levelValidator`. `compress(ct, data)` gives the same bytes
/// as this function with [`CompressionType::default_level`].
///
/// - gzip: the deflate level. -1 is the zlib default, which is level 6.
/// - zstd: the zstd level.
/// - lz4: Kafka's `Lz4BlockOutputStream` uses the fast compressor at the
///   default level 9, and LZ4 HC for every other level in `1..=17`. This
///   function matches that split.
///
/// # Errors
///
/// Returns [`CompressionError::InvalidLevel`] when the level is out of range,
/// or when the codec (`none`, `snappy`) has no levels. Returns
/// [`CompressionError::FeatureDisabled`] when the codec's Cargo feature is not
/// enabled.
pub fn compress_with_level(
    ct: CompressionType,
    data: &[u8],
    level: i32,
) -> Result<Bytes, CompressionError> {
    ct.check_level(level)?;
    match ct {
        CompressionType::Gzip => gzip_compress_with_level(data, level),
        CompressionType::Lz4 => lz4_compress_with_level(data, level),
        CompressionType::Zstd => zstd_compress_with_level(data, level),
        // `check_level` rejects the codecs without levels.
        CompressionType::None | CompressionType::Snappy => compress(ct, data),
    }
}

/// Decompress `data` using the codec identified by `ct`. See `compress`.
///
/// `max_output` bounds the size of the decompressed output. If decompression
/// would produce more than `max_output` bytes, the function returns
/// `Err(CompressionError::TooLarge { .. })` and does not materialize the
/// oversized buffer. This guards against decompression bombs on the untrusted
/// decode path. Callers that handle wire input should derive `max_output` from
/// the compressed length, for example from a bounded ratio plus an absolute
/// ceiling.
/// # Errors
/// Returns an error when input is malformed, compression or decompression fails, or runtime rate-limit state cannot be updated.
pub fn decompress(
    ct: CompressionType,
    data: &[u8],
    max_output: ByteSize,
) -> Result<Bytes, CompressionError> {
    // The per-codec decoders compare against buffer lengths and size
    // allocations, so they keep the exact `usize` count.
    let max_output = max_output.bytes_usize();
    match ct {
        CompressionType::None => {
            if data.len() > max_output {
                Err(CompressionError::TooLarge { limit: max_output })
            } else {
                Ok(Bytes::copy_from_slice(data))
            }
        }
        CompressionType::Gzip => gzip_decompress(data, max_output),
        CompressionType::Snappy => snappy_decompress(data, max_output),
        CompressionType::Lz4 => lz4_decompress(data, max_output),
        CompressionType::Zstd => zstd_decompress(data, max_output),
    }
}

// --- per-codec dispatch, with feature-gated stubs ------------------------

#[cfg(feature = "gzip")]
mod gzip;
#[cfg(feature = "gzip")]
use crate::gzip::{
    compress as gzip_compress, compress_with_level as gzip_compress_with_level,
    decompress as gzip_decompress,
};
#[cfg(not(feature = "gzip"))]
fn gzip_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}
#[cfg(not(feature = "gzip"))]
fn gzip_compress_with_level(_: &[u8], _: i32) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}
#[cfg(not(feature = "gzip"))]
fn gzip_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("gzip"))
}

#[cfg(feature = "snappy")]
mod snappy;
#[cfg(feature = "snappy")]
use crate::snappy::{compress as snappy_compress, decompress as snappy_decompress};
#[cfg(not(feature = "snappy"))]
fn snappy_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}
#[cfg(not(feature = "snappy"))]
fn snappy_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("snappy"))
}

#[cfg(feature = "lz4")]
mod lz4;
#[cfg(feature = "lz4")]
use crate::lz4::{
    compress as lz4_compress, compress_with_level as lz4_compress_with_level,
    decompress as lz4_decompress,
};
#[cfg(not(feature = "lz4"))]
fn lz4_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}
#[cfg(not(feature = "lz4"))]
fn lz4_compress_with_level(_: &[u8], _: i32) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}
#[cfg(not(feature = "lz4"))]
fn lz4_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("lz4"))
}

#[cfg(feature = "zstd")]
mod zstd;
#[cfg(feature = "zstd")]
use crate::zstd::{
    compress as zstd_compress, compress_with_level as zstd_compress_with_level,
    decompress as zstd_decompress,
};
#[cfg(not(feature = "zstd"))]
fn zstd_compress(_: &[u8]) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}
#[cfg(not(feature = "zstd"))]
fn zstd_compress_with_level(_: &[u8], _: i32) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}
#[cfg(not(feature = "zstd"))]
fn zstd_decompress(_: &[u8], _: usize) -> Result<Bytes, CompressionError> {
    Err(CompressionError::FeatureDisabled("zstd"))
}

#[cfg(test)]
mod tests {
    use krabka_units::{bytes, fraction, gibibytes, kibibytes, mebibytes};

    use super::*;

    #[test]
    fn passthrough_none_compress() {
        let out = compress(CompressionType::None, b"abcdef").unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress() {
        let out = decompress(CompressionType::None, b"abcdef", kibibytes(1)).unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    #[test]
    fn passthrough_none_decompress_respects_cap() {
        // Input larger than the cap is rejected even for the None passthrough.
        assert2::assert!(matches!(
            decompress(CompressionType::None, b"abcdef", bytes(3)),
            Err(CompressionError::TooLarge { limit: 3 })
        ));
    }

    #[test]
    fn passthrough_none_decompress_at_exact_cap() {
        // Boundary: input of exactly `max_output` bytes is allowed (the cap
        // check is `len > max_output`, not `>=`).
        let out = decompress(CompressionType::None, b"abcdef", bytes(6)).unwrap();
        assert2::assert!(out.as_ref() == b"abcdef");
    }

    /// The level reaches the codec: a payload that the codec compresses
    /// better at a higher level gives different bytes at the lowest and the
    /// highest level, every output decompresses to the input, and the default
    /// level gives the bytes of `compress`. Lz4's default level (9) also gives
    /// the bytes of `compress`, but its other levels use HC, which differs
    /// from the fast compressor at the low and high ends of its range.
    #[test]
    fn compress_with_level_applies_the_level() {
        let payload: Vec<u8> = (0..64 * 1024u32)
            .map(|i| u8::try_from((i * 7 + i / 13) % 251).unwrap_or(0))
            .collect();
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (codec, low, high, levels_differ) in [
            (CompressionType::Gzip, 1, 9, true),
            (CompressionType::Zstd, 1, 19, true),
            (CompressionType::Lz4, 1, 17, true),
        ] {
            let default = compress(codec, &payload).unwrap();
            let at_default =
                compress_with_level(codec, &payload, codec.default_level().unwrap()).unwrap();
            let at_low = compress_with_level(codec, &payload, low).unwrap();
            let at_high = compress_with_level(codec, &payload, high).unwrap();
            let round_trips = [&at_default, &at_low, &at_high].iter().all(|out| {
                decompress(codec, out, mebibytes(1)).unwrap().as_ref() == payload.as_slice()
            });
            actual.push((codec, at_default == default, at_low != at_high, round_trips));
            expected.push((codec, true, levels_differ, true));
        }
        assert2::assert!(actual == expected);
    }

    #[test]
    fn compress_with_level_rejects_a_level_kafka_rejects() {
        for (codec, level, reason) in [
            (
                CompressionType::Gzip,
                0,
                "Value must be between 1 and 9 or equal to -1",
            ),
            (CompressionType::Lz4, 18, "Value must be no more than 17"),
            (CompressionType::Zstd, 23, "Value must be no more than 22"),
            (
                CompressionType::Snappy,
                1,
                "Compression levels are not defined for this compression type: snappy",
            ),
        ] {
            let error = compress_with_level(codec, b"payload", level).unwrap_err();
            assert2::assert!(
                matches!(
                    &error,
                    CompressionError::InvalidLevel { codec: name, level: got, reason: text }
                        if *name == codec.name() && *got == level && text == reason
                ),
                "{error:?}"
            );
        }
    }

    #[test]
    fn record_policy_preserves_existing_budget_curve() {
        let policy = RecordDecompressionPolicy::default();
        assert2::check!(policy.output_limit(bytes(1)) == mebibytes(16));
        assert2::check!(policy.output_limit(mebibytes(1)) == mebibytes(100));
        assert2::check!(policy.output_limit(mebibytes(11)) == gibibytes(1));
    }

    /// Every existing test reaches the policy through `default()` or
    /// `output_limit()`, so nothing asserted that `new()` accepts a valid
    /// policy, and nothing read the three accessors back.
    ///
    /// Both halves matter. `validated_whole_bytes` rejects fractional byte
    /// counts through `raw.fract() != 0.0`; read as `== 0.0`, or with the
    /// `!is_finite()` guard beside it inverted, it rejects the whole numbers
    /// instead -- which only a successful construction notices, because the
    /// existing test asserts that bad input is refused, not that good input is
    /// taken. Reading the accessors back is what distinguishes them from
    /// `Default::default()`, which for these types is a plausible-looking zero.
    #[test]
    fn record_policy_accepts_whole_byte_bounds_and_reports_them_back() {
        let policy = RecordDecompressionPolicy::new(fraction(50.0), mebibytes(8), gibibytes(1))
            .expect("a finite in-range ratio with whole-byte bounds is a valid policy");

        assert2::check!(policy.max_ratio() == fraction(50.0));
        assert2::check!(policy.output_floor() == mebibytes(8));
        assert2::check!(policy.output_ceiling() == gibibytes(1));
    }

    #[test]
    fn record_policy_rejects_invalid_or_weakened_security_bounds() {
        for result in [
            RecordDecompressionPolicy::new(fraction(0.0), mebibytes(16), gibibytes(1)),
            RecordDecompressionPolicy::new(fraction(101.0), mebibytes(16), gibibytes(1)),
            RecordDecompressionPolicy::new(fraction(100.0), gibibytes(1), mebibytes(16)),
            RecordDecompressionPolicy::new(fraction(100.0), mebibytes(16), gibibytes(2)),
            RecordDecompressionPolicy::new(
                fraction(100.0),
                ByteSize::from_bytes_f64(0.5),
                gibibytes(1),
            ),
        ] {
            assert2::check!(result.is_err());
        }
    }
}
