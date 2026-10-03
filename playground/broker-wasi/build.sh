#!/usr/bin/env bash
# Build the real krabka-broker for the Cluster Lab and stage it for the site.
#
# The script builds `krabka-broker-wasi` for `wasm32-wasip1` (release) and
# copies the module to `public/playground/broker/krabka-broker.wasm`, where the
# lab page loads it from. Astro copies `public/` into `dist/` unchanged. The
# staged module is git-ignored; `npm run build:broker` regenerates it, locally
# and in the deploy workflow, which runs it before the site build.
#
# The C code in the broker's graph (ring, zstd and LZ4) compiles with clang
# against the WASI sysroot of wasi-sdk 25. The script keeps
# `CC_wasm32_wasip1`, `AR_wasm32_wasip1` and `CFLAGS_wasm32_wasip1` when they
# are set, as the workflows set them. Otherwise it takes `clang`, the
# `llvm-ar` that clang finds (or the newest `/usr/lib/llvm-*/bin/llvm-ar`), and
# the sysroot at `$WASI_SYSROOT`, which it downloads into `.tools/` when that
# variable is unset.
set -euo pipefail

crate_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${crate_dir}/../.." && pwd)"
out_dir="${repo_root}/public/playground/broker"
tools_dir="${crate_dir}/.tools"
sysroot_release="wasi-sdk-25"
sysroot_name="wasi-sysroot-25.0"

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
if [[ -z "${CC_wasm32_wasip1:-}" ]]; then
  if ! command -v clang >/dev/null 2>&1; then
    echo "error: clang is not on PATH; the C code of ring, zstd and LZ4 needs it." >&2
    exit 1
  fi
  export CC_wasm32_wasip1=clang
fi
if [[ -z "${AR_wasm32_wasip1:-}" ]]; then
  # `clang -print-prog-name` gives the archiver clang finds for itself, or
  # only the bare name when it finds none; then take the newest LLVM's.
  ar="$(clang -print-prog-name=llvm-ar 2>/dev/null || true)"
  if [[ -z "${ar}" ]] || ! command -v "${ar}" >/dev/null 2>&1; then
    ar="$(ls /usr/lib/llvm-*/bin/llvm-ar 2>/dev/null | sort -V | tail -n 1 || true)"
  fi
  if [[ -z "${ar}" ]]; then
    echo "error: no llvm-ar found; set AR_wasm32_wasip1." >&2
    exit 1
  fi
  export AR_wasm32_wasip1="${ar}"
fi
if [[ -z "${CFLAGS_wasm32_wasip1:-}" ]]; then
  sysroot="${WASI_SYSROOT:-}"
  if [[ -z "${sysroot}" ]]; then
    sysroot="${tools_dir}/${sysroot_name}"
    if [[ ! -d "${sysroot}/include" ]]; then
      url="https://github.com/WebAssembly/wasi-sdk/releases/download/${sysroot_release}/${sysroot_name}.tar.gz"
      echo "    downloading ${url}"
      mkdir -p "${tools_dir}"
      curl -LsSf --retry 5 --retry-all-errors "${url}" | tar xz -C "${tools_dir}"
    fi
  fi
  export CFLAGS_wasm32_wasip1="--sysroot=${sysroot}"
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
  /*) ;;
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
