#!/usr/bin/env bash
# Source this file. Explicit caller toolchains remain outside download verification.
WASI_SDK_MANAGED_DIR=""
repo_tools="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "${CC_wasm32_wasip1:-}" || -z "${AR_wasm32_wasip1:-}" || -z "${CFLAGS_wasm32_wasip1:-}" ]]; then
  if [[ -z "${WASI_SDK_PATH:-}" ]]; then
    sdk_lock="$(python3 "${repo_tools}/install-tool.py" wasi-sdk --lock-path)" || return 1
    # The shell owns this descriptor. Python takes one POSIX flock on it;
    # closing the installer does not release the shell's copy of that lock.
    exec {WASI_SDK_LOCK_FD}>"${sdk_lock}" || return 1
    WASI_SDK_PATH="$(python3 "${repo_tools}/install-tool.py" wasi-sdk --lock-fd "${WASI_SDK_LOCK_FD}")" || return 1
    WASI_SDK_MANAGED_DIR="$(dirname "${WASI_SDK_PATH}")"
  fi
  export WASI_SDK_PATH
  export CC_wasm32_wasip1="${CC_wasm32_wasip1:-${WASI_SDK_PATH}/bin/clang}"
  export AR_wasm32_wasip1="${AR_wasm32_wasip1:-${WASI_SDK_PATH}/bin/llvm-ar}"
  export CFLAGS_wasm32_wasip1="${CFLAGS_wasm32_wasip1:---sysroot=${WASI_SDK_PATH}/share/wasi-sysroot}"
fi
