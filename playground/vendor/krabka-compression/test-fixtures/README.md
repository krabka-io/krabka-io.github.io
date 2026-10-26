# RUSTSEC-2026-0344 regression fixtures

`oversized-sequence.zst` is an 18-byte zstd frame. Its compressed block
expands two literal `a` bytes through a match sequence into 4096 bytes,
although the frame declares a 1024-byte window. The compressed block's
encoded size is valid: rejection must enforce the decoded block bound.

Construction: compress 4096 `a` bytes with libzstd level 1. Replace the
single-segment frame header (hex `28b52ffd60000f`) with a non-single-segment
header with no content-size field and a 1 KiB window (`28b52ffd0000`).
Keep the compressed block (`4d00001061610100fbf70116`) unchanged. Changing
the window descriptor at byte 5 to `0x18` gives an 8 KiB window and a valid
positive control, used by the codec test.

`oversized-sequence.batch` wraps that exact frame in a Kafka v2 record-batch
header: magic 2, zstd attribute 4, one record, batch length 67, and a valid
CRC-32C over bytes 21 onward. Other header fields are zero. The record bytes
are deliberately not a valid record: the test requires the specific zstd
sequence-expansion error, not a record parsing or checksum failure.

These exercise the sequence-execution bound added by upstream:
https://github.com/KillingSpark/zstd-rs/commit/d3907c960ca4a1aa44cc38340137c5679dd98f5a

A one-off comparison under 256 MiB / 10-second process limits confirmed:

- ruzstd 0.9.0: `Ok(4096)`; 4096 output bytes.
- ruzstd 0.9.1: `TooManyBytesGenerated { max: 1024, decoded: 4096 }`;
  no output delivered to the caller.

No large decompression bomb or vulnerable-version subprocess is part of CI.
