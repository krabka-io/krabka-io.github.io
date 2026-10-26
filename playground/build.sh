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
# Explicit overrides are caller-managed; never implicitly execute a PATH binary.
if [[ -n "${WASM_BINDGEN:-}" ]]; then
  wasm_bindgen="${WASM_BINDGEN}"
else
  wasm_bindgen="$(python3 "${repo_root}/scripts/install-tool.py" wasm-bindgen --version "${wb_version}")"
  managed_install="$(dirname "$(dirname "${wasm_bindgen}")")"
  trap 'rm -rf -- "${managed_install}"' EXIT
fi
if [[ "$("${wasm_bindgen}" --version | awk '{print $2}')" != "${wb_version}" ]]; then
  echo "error: wasm-bindgen override does not match Cargo.lock (${wb_version})." >&2
  exit 1
fi

# Run cargo from inside the crate so that its .cargo/config.toml (the
# getrandom backend cfg for wasm32) applies.
echo "==> Building krabka-playground (release)"
(cd "${crate_dir}" && cargo build --locked --target wasm32-unknown-unknown --release)

# Cargo writes to CARGO_TARGET_DIR when it is set, and to the crate's own
# target directory otherwise. Cargo ran inside the crate, so a relative
# CARGO_TARGET_DIR is relative to the crate, not to the caller.
target_dir="${CARGO_TARGET_DIR:-${crate_dir}/target}"
case "${target_dir}" in
  /* | [A-Za-z]:[/]*) ;; # absolute, POSIX or Windows (Git Bash may print D:/...)
  *) target_dir="${crate_dir}/${target_dir}" ;;
esac
wasm_in="${target_dir}/wasm32-unknown-unknown/release/krabka_playground.wasm"

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
  # The features the module uses (rustc's wasm32-unknown-unknown defaults,
  # and SIMD that a dependency enables per function): wasm-bindgen drops the
  # section that would let wasm-opt detect them.
  wasm-opt -Oz \
    --enable-bulk-memory --enable-bulk-memory-opt --enable-sign-ext \
    --enable-mutable-globals --enable-nontrapping-float-to-int \
    --enable-reference-types --enable-multivalue --enable-simd \
    "${out_dir}/krabka_playground_bg.wasm" \
    -o "${out_dir}/krabka_playground_bg.wasm"
fi

echo "==> Playground staged into ${out_dir}:"
ls -lh "${out_dir}/krabka_playground.js" "${out_dir}/krabka_playground_bg.wasm"
