//! `CompressionType` enum mapping to Kafka's record-batch attribute bits.

use crate::CompressionError;

/// Kafka's gzip levels: `Deflater.BEST_SPEED`, `Deflater.BEST_COMPRESSION`
/// and `Deflater.DEFAULT_COMPRESSION`.
const GZIP_LEVELS: LevelRange = LevelRange {
    min: 1,
    max: 9,
    default: -1,
};

/// Kafka's lz4 levels, from `net.jpountz.lz4.LZ4Constants`.
const LZ4_LEVELS: LevelRange = LevelRange {
    min: 1,
    max: 17,
    default: 9,
};

/// Kafka's zstd levels: `ZSTD_minCLevel`, `ZSTD_MAX_CLEVEL` and
/// `ZSTD_CLEVEL_DEFAULT`.
const ZSTD_LEVELS: LevelRange = LevelRange {
    min: -131_072,
    max: 22,
    default: 3,
};

/// The levels of one codec.
#[derive(Debug, Clone, Copy)]
struct LevelRange {
    min: i32,
    max: i32,
    default: i32,
}

/// Codec identifier matching the lowest three bits of Kafka's record-batch
/// attribute byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    /// Decode the lowest three bits of a Kafka record-batch attribute byte.
    ///
    /// The method returns `None` for codec ids outside `0..=4`.
    #[must_use]
    pub fn from_attribute_bits(b: u8) -> Option<Self> {
        match b & 0b0000_0111 {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Encode this codec into the lowest three bits of an attribute byte.
    #[must_use]
    pub fn as_attribute_bits(self) -> u8 {
        self as u8
    }

    /// The codec name that Kafka uses in `compression.type`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// Kafka's default compression level of the codec
    /// (`CompressionType.defaultLevel`), or `None` when the codec has no
    /// levels.
    #[must_use]
    pub const fn default_level(self) -> Option<i32> {
        match self.levels() {
            Some(levels) => Some(levels.default),
            None => None,
        }
    }

    /// Kafka's lowest compression level of the codec
    /// (`CompressionType.minLevel`), or `None` when the codec has no levels.
    #[must_use]
    pub const fn min_level(self) -> Option<i32> {
        match self.levels() {
            Some(levels) => Some(levels.min),
            None => None,
        }
    }

    /// Kafka's highest compression level of the codec
    /// (`CompressionType.maxLevel`), or `None` when the codec has no levels.
    #[must_use]
    pub const fn max_level(self) -> Option<i32> {
        match self.levels() {
            Some(levels) => Some(levels.max),
            None => None,
        }
    }

    /// Check `level` as Kafka's `CompressionType.levelValidator` does (KIP-390).
    ///
    /// Gzip accepts 1 to 9, and also -1, which is the zlib default. Lz4 and
    /// zstd accept their range only.
    ///
    /// # Errors
    ///
    /// Returns [`CompressionError::InvalidLevel`] with Kafka's message when the
    /// level is outside the range, or when the codec has no levels.
    pub fn check_level(self, level: i32) -> Result<(), CompressionError> {
        let invalid = |reason: String| CompressionError::InvalidLevel {
            codec: self.name(),
            level,
            reason,
        };
        match (self, self.levels()) {
            (_, None) => Err(invalid(format!(
                "Compression levels are not defined for this compression type: {}",
                self.name()
            ))),
            (Self::Gzip, Some(levels)) => {
                if level > levels.max || (level < levels.min && level != levels.default) {
                    Err(invalid(format!(
                        "Value must be between {} and {} or equal to {}",
                        levels.min, levels.max, levels.default
                    )))
                } else {
                    Ok(())
                }
            }
            (_, Some(levels)) => {
                if level < levels.min {
                    Err(invalid(format!("Value must be at least {}", levels.min)))
                } else if level > levels.max {
                    Err(invalid(format!(
                        "Value must be no more than {}",
                        levels.max
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }

    const fn levels(self) -> Option<LevelRange> {
        match self {
            Self::None | Self::Snappy => None,
            Self::Gzip => Some(GZIP_LEVELS),
            Self::Lz4 => Some(LZ4_LEVELS),
            Self::Zstd => Some(ZSTD_LEVELS),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn attribute_bits_roundtrip() {
        for (_name, compression) in [
            ("none", CompressionType::None),
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
            ("zstd", CompressionType::Zstd),
        ] {
            assert2::assert!(
                CompressionType::from_attribute_bits(compression.as_attribute_bits())
                    == Some(compression)
            );
        }
    }

    /// Each row follows Kafka's `CompressionType` level constants and
    /// `levelValidator` (gzip), or `ConfigDef.Range.ensureValid` (lz4, zstd).
    #[test]
    fn levels_follow_kafka_compression_type() {
        let reasons = |codec: CompressionType, levels: &[i32]| -> Vec<Option<String>> {
            levels
                .iter()
                .map(|&level| match codec.check_level(level) {
                    Ok(()) => None,
                    Err(CompressionError::InvalidLevel { reason, .. }) => Some(reason),
                    Err(other) => Some(format!("unexpected error {other}")),
                })
                .collect()
        };
        let gzip_reason = Some("Value must be between 1 and 9 or equal to -1".to_owned());
        let none_levels = |name: &str| {
            Some(format!(
                "Compression levels are not defined for this compression type: {name}"
            ))
        };
        let actual = [
            (
                "none",
                CompressionType::None.default_level(),
                CompressionType::None.min_level(),
                CompressionType::None.max_level(),
                reasons(CompressionType::None, &[0]),
            ),
            (
                "gzip",
                CompressionType::Gzip.default_level(),
                CompressionType::Gzip.min_level(),
                CompressionType::Gzip.max_level(),
                reasons(CompressionType::Gzip, &[-2, -1, 0, 1, 9, 10]),
            ),
            (
                "snappy",
                CompressionType::Snappy.default_level(),
                CompressionType::Snappy.min_level(),
                CompressionType::Snappy.max_level(),
                reasons(CompressionType::Snappy, &[1]),
            ),
            (
                "lz4",
                CompressionType::Lz4.default_level(),
                CompressionType::Lz4.min_level(),
                CompressionType::Lz4.max_level(),
                reasons(CompressionType::Lz4, &[0, 1, 17, 18]),
            ),
            (
                "zstd",
                CompressionType::Zstd.default_level(),
                CompressionType::Zstd.min_level(),
                CompressionType::Zstd.max_level(),
                reasons(CompressionType::Zstd, &[-131_073, -131_072, 22, 23]),
            ),
        ];
        let expected = [
            ("none", None, None, None, vec![none_levels("none")]),
            (
                "gzip",
                Some(-1),
                Some(1),
                Some(9),
                vec![
                    gzip_reason.clone(),
                    None,
                    gzip_reason.clone(),
                    None,
                    None,
                    gzip_reason,
                ],
            ),
            ("snappy", None, None, None, vec![none_levels("snappy")]),
            (
                "lz4",
                Some(9),
                Some(1),
                Some(17),
                vec![
                    Some("Value must be at least 1".to_owned()),
                    None,
                    None,
                    Some("Value must be no more than 17".to_owned()),
                ],
            ),
            (
                "zstd",
                Some(3),
                Some(-131_072),
                Some(22),
                vec![
                    Some("Value must be at least -131072".to_owned()),
                    None,
                    None,
                    Some("Value must be no more than 22".to_owned()),
                ],
            ),
        ];
        assert2::assert!(actual == expected);
    }

    #[test]
    fn attribute_bits_mask() {
        // Only the low 3 bits define the codec; upper bits are other flags.
        assert2::assert!(
            CompressionType::from_attribute_bits(0b1111_1000 | 0b0000_0001)
                == Some(CompressionType::Gzip)
        );
    }

    #[test]
    fn attribute_bits_unknown() {
        for (_name, bits) in [("reserved five", 5), ("reserved seven", 7)] {
            assert2::assert!(CompressionType::from_attribute_bits(bits) == None);
        }
    }
}
