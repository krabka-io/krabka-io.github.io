//! Kafka's partition choice: `murmur2` and `Utils.toPositive` for a keyed
//! record, the sticky partitioner of KIP-480 for a keyless one, and Java's
//! `String.hashCode`.
//!
//! A JVM producer and this one must put the same key on the same partition,
//! so the hash is the reference `MurmurHash2` with Kafka's seed, and the
//! partition is `toPositive(murmur2(key)) % numPartitions`: the sign bit is
//! masked, not the absolute value taken.

use std::collections::BTreeMap;

/// `MurmurHash2` with Kafka's seed `0x9747b28c`: `Utils.murmur2`.
#[must_use]
pub fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    // Kafka mixes the length in as a 32-bit value; a Java array's length is
    // an `int`, so no real input exceeds `u32`.
    let length = u32::try_from(data.len()).unwrap_or(u32::MAX);
    let mut h: u32 = SEED ^ length;
    let (chunks, remainder) = data.as_chunks::<4>();
    for chunk in chunks {
        let mut k = u32::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match remainder {
        [a, b, c] => {
            h ^= u32::from(*c) << 16;
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(M);
        }
        [a, b] => {
            h ^= u32::from(*b) << 8;
            h ^= u32::from(*a);
            h = h.wrapping_mul(M);
        }
        [a] => {
            h ^= u32::from(*a);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h.cast_signed()
}

/// `Utils.toPositive`: the value with its sign bit cleared.
#[must_use]
pub const fn to_positive(n: i32) -> i32 {
    n & 0x7fff_ffff
}

/// The partition of a keyed record, as Kafka's `BuiltInPartitioner` picks
/// it, or `None` when the topic has no partitions.
#[must_use]
pub fn partition_for_key(key: &[u8], num_partitions: i32) -> Option<i32> {
    (num_partitions > 0).then(|| to_positive(murmur2(key)) % num_partitions)
}

/// Java's `String.hashCode`: `s[0]*31^(n-1) + ... + s[n-1]` over the UTF-16
/// code units, with wrapping arithmetic. Kafka's group coordinator maps a
/// group id to its offsets partition with it.
#[must_use]
pub fn java_string_hash_code(s: &str) -> i32 {
    s.encode_utf16().fold(0_i32, |h, unit| {
        h.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// The sticky partition of one topic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sticky {
    partition: i32,
    records: u32,
}

/// The sticky partitioner of keyless records (KIP-480): a topic keeps one
/// partition for a run of records, then moves to another one at random.
///
/// Kafka moves after `batch.size` bytes went to the partition; the lab moves
/// after `batch_records` records, which keeps the visible behaviour (runs of
/// records land together, then the run changes partition) without a byte
/// budget. As in Kafka's `BuiltInPartitioner.nextPartition`, the new
/// partition is picked among the partitions that have a leader and is never
/// the one just left when another is available.
#[derive(Clone, Debug)]
pub struct StickyPartitioner {
    batch_records: u32,
    topics: BTreeMap<String, Sticky>,
}

impl StickyPartitioner {
    /// A partitioner that moves after `batch_records` records per topic.
    #[must_use]
    pub fn new(batch_records: u32) -> Self {
        Self {
            batch_records: batch_records.max(1),
            topics: BTreeMap::new(),
        }
    }

    /// The partition of the next keyless record of `topic`. `available`
    /// lists the partitions with a leader, `partition_count` every partition,
    /// and `random` is the node's next random value.
    pub fn partition(
        &mut self,
        topic: &str,
        available: &[i32],
        partition_count: i32,
        random: u64,
    ) -> i32 {
        let sticky = self.topics.entry(topic.to_string()).or_insert(Sticky {
            partition: -1,
            records: 0,
        });
        let lost_leader = !available.is_empty() && !available.contains(&sticky.partition);
        if sticky.partition < 0 || sticky.records >= self.batch_records || lost_leader {
            sticky.partition = pick(available, partition_count, random, sticky.partition);
            sticky.records = 0;
        }
        sticky.records += 1;
        sticky.partition
    }

    /// The current sticky partition of a topic.
    #[must_use]
    pub fn current(&self, topic: &str) -> Option<i32> {
        self.topics
            .get(topic)
            .map(|s| s.partition)
            .filter(|p| *p >= 0)
    }
}

/// A new partition: among `available`, or among every partition when none
/// has a leader; never `previous` when another choice exists.
fn pick(available: &[i32], partition_count: i32, random: u64, previous: i32) -> i32 {
    let random = usize::try_from(random).unwrap_or(0);
    if available.is_empty() {
        let count = usize::try_from(partition_count.max(1)).unwrap_or(1);
        let mut index = random % count;
        if count > 1 && i32::try_from(index).unwrap_or(-1) == previous {
            index = (index + 1) % count;
        }
        return i32::try_from(index).unwrap_or(0);
    }
    let mut index = random % available.len();
    if available.len() > 1 && available[index] == previous {
        index = (index + 1) % available.len();
    }
    available[index]
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The first six rows are vectors of Apache Kafka's
    /// `UtilsTest.testMurmur2` (`"21"`, `"foobar"`,
    /// `"a-little-bit-long-string"`, `"a-little-bit-longer-string"`,
    /// `{'a','b','c'}` and the empty array). The rest are the golden vectors
    /// the krabka broker (`kafka_hash.rs`) and the krabka client
    /// (`partitioner.rs`) pin, one per remainder length.
    #[test]
    fn murmur2_matches_kafkas_utils_test_vectors() {
        let cases: [(&[u8], i32); 12] = [
            (b"21", -973_932_308),
            (b"foobar", -790_332_482),
            (b"a-little-bit-long-string", -985_981_536),
            (b"a-little-bit-longer-string", -1_486_304_829),
            (b"abc", 479_470_107),
            (b"", 275_646_681),
            (b"a", -1_563_381_124),
            (b"ab", 316_155_434),
            (b"abcd", -1_323_649_548),
            (b"abcde", 461_995_741),
            (b"kafka", -798_503_068),
            (b"my-key", 1_748_425_209),
        ];
        for (input, expected) in cases {
            assert!(murmur2(input) == expected, "{input:?}");
        }
    }

    #[test]
    fn keyed_partitions_mask_the_sign_bit_like_utils_to_positive() {
        // "kafka" hashes negative; the mask gives 1348980580, an absolute
        // value would give 798503068, and the two land on different
        // partitions.
        assert!(to_positive(-798_503_068) == 1_348_980_580);
        let cases: [(&[u8], i32, i32); 12] = [
            (b"", 10, 1),
            (b"a", 10, 4),
            (b"ab", 10, 4),
            (b"abc", 10, 7),
            (b"abcd", 10, 0),
            (b"abcde", 10, 1),
            (b"kafka", 10, 0),
            (b"my-key", 10, 9),
            (b"abcd", 16, 4),
            (b"kafka", 16, 4),
            (b"abcd", 3, 2),
            (b"kafka", 3, 1),
        ];
        for (key, partitions, expected) in cases {
            assert!(
                partition_for_key(key, partitions) == Some(expected),
                "{key:?} over {partitions}"
            );
        }
        assert!(partition_for_key(b"x", 0).is_none());
    }

    #[test]
    fn java_string_hash_code_matches_the_jvm() {
        // `String.hashCode` over UTF-16 code units with 32-bit wrapping, the
        // values computed independently of this crate. The emoji is a
        // surrogate pair, two code units.
        let cases = [
            ("", 0),
            ("a", 97),
            ("abc", 96_354),
            ("hello", 99_162_322),
            ("__consumer_offsets", -970_371_369),
            ("billing", -109_829_509),
            ("order-stats", 541_797_728),
            ("\u{1F600}", 1_772_899),
            ("grüße", 98_768_023),
        ];
        for (input, expected) in cases {
            assert!(java_string_hash_code(input) == expected, "{input}");
        }
    }

    #[test]
    fn sticky_partition_holds_for_a_run_then_moves_and_avoids_the_last_one() {
        let mut sticky = StickyPartitioner::new(3);
        let available = [0, 1, 2, 3];
        // Random 1 picks partition 1; the run keeps it for three records.
        let picks: Vec<i32> = (0..3)
            .map(|_| sticky.partition("t", &available, 4, 1))
            .collect();
        assert!(picks == vec![1, 1, 1]);
        assert!(sticky.current("t") == Some(1));
        // The fourth record moves. Random 1 would pick 1 again, so the next
        // partition is taken.
        assert!(sticky.partition("t", &available, 4, 1) == 2);
        // A partition that lost its leader is left at once.
        assert!(sticky.partition("t", &[0, 3], 4, 0) == 0);
        // Without any leader, every partition is a candidate.
        let mut fresh = StickyPartitioner::new(10);
        assert!(fresh.partition("u", &[], 5, 7) == 2);
        assert!(fresh.current("v").is_none());
    }
}
