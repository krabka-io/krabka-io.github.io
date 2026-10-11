#!/usr/bin/env bash
# Build the real krabka-broker for the Cluster Lab and stage it for the site.
#
# The script builds `krabka-broker-wasi` for `wasm32-wasip1` (release) and
# copies the module to `public/playground/broker/krabka-broker.wasm`, where the
# lab page loads it from. Astro copies `public/` into `dist/` unchanged. The
# staged module is git-ignored; `npm run build:broker` regenerates it, locally
# and in the deploy workflow, which runs it before the site build.
#
# The C code in the broker's graph (ring, zstd and LZ4) compiles with the
# clang, llvm-ar and WASI sysroot of wasi-sdk 34, which are built together. The
# script keeps `CC_wasm32_wasip1`, `AR_wasm32_wasip1` and
# `CFLAGS_wasm32_wasip1` when they are set. Otherwise it takes the toolchain
# from the wasi-sdk at `$WASI_SDK_PATH`, which it downloads into `.tools/` for
# the host's OS and architecture when that variable is unset.
set -euo pipefail

crate_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${crate_dir}/../.." && pwd)"
out_dir="${repo_root}/public/playground/broker"
tools_dir="${crate_dir}/.tools"
sdk_release="wasi-sdk-34"
sdk_version="34.0"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not on PATH." >&2
  echo "The broker module is a Rust crate. Install Rust from https://rustup.rs and retry." >&2
  exit 1
fi

echo "==> Target wasm32-wasip1"
if command -v rustup >/dev/null 2>&1; then
  rustup target add wasm32-wasip1 >/dev/null
fi

echo "==> C toolchain for wasm32-wasip1"
if [[ -z "${CC_wasm32_wasip1:-}" || -z "${AR_wasm32_wasip1:-}" || -z "${CFLAGS_wasm32_wasip1:-}" ]]; then
  sdk="${WASI_SDK_PATH:-}"
  if [[ -z "${sdk}" ]]; then
    case "$(uname -s)" in
      Linux) os=linux ;;
      Darwin) os=macos ;;
      *) echo "error: no wasi-sdk build for $(uname -s); set WASI_SDK_PATH." >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
      x86_64 | amd64) arch=x86_64 ;;
      aarch64 | arm64) arch=arm64 ;;
      *) echo "error: no wasi-sdk build for $(uname -m); set WASI_SDK_PATH." >&2; exit 1 ;;
    esac
    sdk_name="wasi-sdk-${sdk_version}-${arch}-${os}"
    sdk="${tools_dir}/${sdk_name}"
    if [[ ! -x "${sdk}/bin/clang" ]]; then
      url="https://github.com/WebAssembly/wasi-sdk/releases/download/${sdk_release}/${sdk_name}.tar.gz"
      echo "    downloading ${url}"
      mkdir -p "${tools_dir}"
      curl -LsSf --retry 5 --retry-all-errors "${url}" | tar xz -C "${tools_dir}"
    fi
  fi
  export CC_wasm32_wasip1="${CC_wasm32_wasip1:-${sdk}/bin/clang}"
  export AR_wasm32_wasip1="${AR_wasm32_wasip1:-${sdk}/bin/llvm-ar}"
  export CFLAGS_wasm32_wasip1="${CFLAGS_wasm32_wasip1:---sysroot=${sdk}/share/wasi-sysroot}"
fi
echo "    CC=${CC_wasm32_wasip1} AR=${AR_wasm32_wasip1} CFLAGS=${CFLAGS_wasm32_wasip1}"

# Run cargo from inside the crate so that its .cargo/config.toml (the
# `--cfg tokio_unstable` of wasm32-wasip1) applies.
echo "==> Building krabka-broker-wasi (release)"
(cd "${crate_dir}" && cargo build --locked --release --target wasm32-wasip1)

# Cargo writes to CARGO_TARGET_DIR when it is set, and to the crate's own
# target directory otherwise. Cargo ran inside the crate, so a relative
# CARGO_TARGET_DIR is relative to the crate, not to the caller.
target_dir="${CARGO_TARGET_DIR:-${crate_dir}/target}"
case "${target_dir}" in
  /* | [A-Za-z]:[/]*) ;; # absolute, POSIX or Windows (Git Bash may print D:/...)
  *) target_dir="${crate_dir}/${target_dir}" ;;
esac
wasm_in="${target_dir}/wasm32-wasip1/release/krabka-broker.wasm"

# No wasm-opt pass: `wasm-opt -Os` takes the module from 13.6 MB to 12.1 MB,
# but gzipped, as the site serves it, only from 4.64 MB to 4.53 MB.
mkdir -p "${out_dir}"
wasm_out="${out_dir}/krabka-broker.wasm"
cp "${wasm_in}" "${wasm_out}"

echo "==> Broker staged into ${out_dir}:"
ls -l "${wasm_out}"
