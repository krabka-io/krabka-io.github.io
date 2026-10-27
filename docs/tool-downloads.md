# Verified native tool downloads

The playground builds and snippets CI use `scripts/install-tool.py` for native
release downloads. The installer needs Python 3.9 or later, HTTPS access, and a
writable cache directory. It uses Python's SHA-256 and tar readers, not GNU-only
shell commands. Ubuntu CI supplies Python 3. Local developers need `python3` on
`PATH`, including in Windows Git Bash.

## Platforms

- wasm-bindgen: Linux x86_64 and ARM64, macOS x86_64 and ARM64, Windows x86_64
  through Git Bash. The CLI version must match `playground/Cargo.lock`.
- wasi-sdk: Linux x86_64 and ARM64, macOS x86_64 and ARM64.
- kind: Linux x86_64, for the snippets Helm job.

Other platforms fail before a download. The installer does not substitute a
binary for a different platform.

## Verification and cache reuse

`scripts/tool-pins.json` records each version, platform, HTTPS URL, archive root,
executable path, and SHA-256 digest. The provenance URL identifies the upstream
GitHub release API response. The initial pins use that response's asset `digest`
field, reviewed on 26 October 2026. These are upstream-published digests, not
independent signatures. A build trusts the reviewed repository pins; it does not
fetch expected checksums from the network.

Downloads go to an isolated staging directory. The installer checks the complete
file against its pin before it extracts or installs anything. A missing pin,
unknown platform, download failure, or checksum mismatch stops the caller.
Archive checks reject absolute paths, traversal, special files, hard links,
duplicate paths, Windows device names and trailing-dot/space components,
escaping or cyclic symlinks, and writes below symlinks. All
headers pass these checks before extraction starts.

The cache is `playground/.tools/verified/`. Download names include the tool,
version, platform, and digest. Every use hashes the cached download again and
extracts a fresh installation. The installer never trusts old extracted files
or checksum markers. Legacy `.tools/` installations are not reused. A corrupt
cached download fails closed; remove the reported file before a retry.

Managed SDK installations use a stable absolute path:

```text
playground/.tools/verified/wasi-sdk-<version>-<host>-<sha>/<archive-root>/
```

Each build clears only that per-digest installation and reconstructs it from the
verified archive. The cached `.download` stays outside that directory. Stable
compiler and sysroot paths remove one source of Cargo invalidation; they do not
guarantee a cache hit or a particular speedup.

The SDK environment helper opens one per-digest `.lock` beside the installation.
The Python installer takes a POSIX `flock` on the inherited descriptor. The build
shell retains ownership through compilation and its exit cleanup. A concurrent
local build waits before reconstruction. The lock file stays outside the wiped
directory and is not deleted. Linux and macOS use Python's `fcntl`; no external
locking utility is required. Explicit caller-managed SDKs do not take this lock.

The broker build removes only its owned per-digest SDK directory on exit, never
the shared cache root. The playground CI job has a private runner, installs once,
and leaves the SDK available for its later steps. Other tools keep isolated
`*-install-*` directories. Remove interrupted installations only when no build
uses them; retain `.download` archives to avoid another network download.

The cache and checkout should be writable only by the developer or CI job.
Verification does not protect against another process that changes files between
verification and use. The installer is not a sandbox for a malicious pinned tool.

## Caller-managed toolchains

`WASM_BINDGEN=/absolute/path/to/wasm-bindgen` selects an explicit local CLI.
The build checks its version against Cargo.lock. A matching binary on `PATH`
is not selected automatically, locally or in CI.

`WASI_SDK_PATH` selects an explicit SDK. Individual `CC_wasm32_wasip1`,
`AR_wasm32_wasip1`, and `CFLAGS_wasm32_wasip1` values remain supported. Explicit
overrides are caller-managed and do not receive download verification. A stable caller-managed SDK path can avoid these path changes, but its contents
remain outside installer verification. CI uses the pinned installer unless a
workflow explicitly supplies an override.

`scripts/wasi-sdk-env.sh` supplies the SDK environment for the broker build and
the playground workflow's clippy target. The deploy workflow uses the broker
build directly. There is no separate workflow download implementation.

## Update a version or digest

1. Select the upstream release and review its release notes and source.
2. Read the asset metadata from the upstream release API URL in `provenance`.
   Select the exact archive for every supported platform. Check each published
   SHA-256 against the downloaded asset, or an upstream checksum/signature where
   available. Do not copy a digest from an unreviewed mirror.
3. Update the version, URLs, archive roots, executable paths, and SHA-256 values
   together in `scripts/tool-pins.json`. Record any changed provenance source.
4. For wasm-bindgen, update Cargo.lock and all five platform entries together.
   The website contract test rejects a mismatched version or incomplete matrix.
5. Run `node --test scripts/tool-downloads.test.mjs`, `npm run test:website`,
   both WASM builds, and the affected snippets suite. Test a clean cache and
   cache reuse. Review the version and digests as part of the pull request.

The policy test inventories shell download commands across workflows and their
helpers. It also checks the native installer call sites. This is a regression
guard, not a general shell or network-flow analyzer. Review new network clients
and indirect executable downloads during code review. Existing Why3 downloads
retain their separate committed pins and checksum checks.
