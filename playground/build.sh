#!/usr/bin/env bash
# Compile the WASM consensus playground into the Astro static directory.
#
# The script builds `krabka-playground` for `wasm32-unknown-unknown`, then runs
# `wasm-bindgen --target web` to emit an ES module and a `.wasm` file. Both land
# in `public/playground/`, next to the hand-written front-end `app.js` and
# `playground.css`. Astro copies `public/` into `dist/` unchanged.
#
# The generated `krabka_playground.js` and `krabka_playground_bg.wasm` are
# git-ignored. `npm run build` regenerates them, locally and in CI.
#
# The script needs `cargo`. It adds the `wasm32-unknown-unknown` target when
# `rustup` is available, and it downloads the `wasm-bindgen` CLI that matches
# the `wasm-bindgen` version in `playground/Cargo.lock`.
set -euo pipefail

crate_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${crate_dir}/.." && pwd)"
out_dir="${repo_root}/public/playground"
tools_dir="${crate_dir}/.tools"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is not on PATH." >&2
  echo "The playground is a Rust crate. Install Rust from https://rustup.rs and retry." >&2
  exit 1
fi

# The CLI and the crate must be the same version, or wasm-bindgen refuses the
# module. Read the crate version straight out of the lock file.
wb_version="$(
  awk '/^name = "wasm-bindgen"$/ { getline; gsub(/[":]/, "", $3); print $3; exit }' \
    "${crate_dir}/Cargo.lock"
)"
if [[ -z "${wb_version}" ]]; then
  echo "error: no wasm-bindgen version in ${crate_dir}/Cargo.lock." >&2
  exit 1
fi

echo "==> Target wasm32-unknown-unknown"
if command -v rustup >/dev/null 2>&1; then
  rustup target add wasm32-unknown-unknown >/dev/null
fi

echo "==> wasm-bindgen CLI ${wb_version}"
wasm_bindgen=""
if command -v wasm-bindgen >/dev/null 2>&1 &&
  [[ "$(wasm-bindgen --version | awk '{print $2}')" == "${wb_version}" ]]; then
  wasm_bindgen="$(command -v wasm-bindgen)"
else
  cached="${tools_dir}/${wb_version}/wasm-bindgen"
  if [[ ! -x "${cached}" ]]; then
    triple="x86_64-unknown-linux-musl"
    case "$(uname -s)-$(uname -m)" in
      Darwin-arm64) triple="aarch64-apple-darwin" ;;
      Darwin-x86_64) triple="x86_64-apple-darwin" ;;
      Linux-aarch64) triple="aarch64-unknown-linux-gnu" ;;
    esac
    name="wasm-bindgen-${wb_version}-${triple}"
    url="https://github.com/wasm-bindgen/wasm-bindgen/releases/download/${wb_version}/${name}.tar.gz"
    echo "    downloading ${url}"
    mkdir -p "${tools_dir}/${wb_version}"
    curl -sSfL "${url}" | tar xz -C "${tools_dir}/${wb_version}" --strip-components=1 "${name}/wasm-bindgen"
  fi
  wasm_bindgen="${cached}"
fi

echo "==> Building krabka-playground (release)"
cargo build \
  --manifest-path "${crate_dir}/Cargo.toml" \
  --target wasm32-unknown-unknown \
  --release

wasm_in="${crate_dir}/target/wasm32-unknown-unknown/release/krabka_playground.wasm"

echo "==> Running wasm-bindgen --target web"
mkdir -p "${out_dir}"
"${wasm_bindgen}" \
  --target web \
  --no-typescript \
  --out-dir "${out_dir}" \
  --out-name krabka_playground \
  "${wasm_in}"

# Size optimisation when wasm-opt (binaryen) is on PATH. It is optional.
if command -v wasm-opt >/dev/null 2>&1; then
  echo "==> Optimising with wasm-opt -Oz"
  wasm-opt -Oz \
    "${out_dir}/krabka_playground_bg.wasm" \
    -o "${out_dir}/krabka_playground_bg.wasm"
fi

echo "==> Playground staged into ${out_dir}:"
ls -lh "${out_dir}/krabka_playground.js" "${out_dir}/krabka_playground_bg.wasm"
