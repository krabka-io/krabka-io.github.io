#!/usr/bin/env bash
# Source this file. Explicit caller toolchains remain outside download verification.
WASI_SDK_MANAGED_DIR=""
repo_tools="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${CC_wasm32_wasip1:-}" || -z "${AR_wasm32_wasip1:-}" || -z "${CFLAGS_wasm32_wasip1:-}" ]]; then
  if [[ -z "${WASI_SDK_PATH:-}" ]]; then
    WASI_SDK_PATH="$(python3 "${repo_tools}/install-tool.py" wasi-sdk)" || return 1
    WASI_SDK_MANAGED_DIR="$(dirname "${WASI_SDK_PATH}")"
  fi
  export WASI_SDK_PATH
  export CC_wasm32_wasip1="${CC_wasm32_wasip1:-${WASI_SDK_PATH}/bin/clang}"
  export AR_wasm32_wasip1="${AR_wasm32_wasip1:-${WASI_SDK_PATH}/bin/llvm-ar}"
  export CFLAGS_wasm32_wasip1="${CFLAGS_wasm32_wasip1:---sysroot=${WASI_SDK_PATH}/share/wasi-sysroot}"
fi
