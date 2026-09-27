#!/bin/bash
# Builds the Why3 web bundle inside the builder container (see BUILD.bazel).
#
# Inputs, all pinned in /opt/why3-web/pins.env: the Why3 release Creusot pins,
# the Alt-Ergo release it pins, and the Creusot commit whose prelude the Coma
# files reference. Output: the tar named by $1 holding
#
#   proof_worker.js      Why3 as a web worker, with its stdlib, the Creusot
#                        prelude, why3.conf and the Alt-Ergo driver embedded
#   alt-ergo-worker.js   Alt-Ergo as a web worker
#   manifest.json        the versions above and the build time
#   LICENSES/            the licences of what the bundle contains
#
# The same steps run by hand on a machine with opam, minus the container:
# why3-web/README.md walks through them.
set -euo pipefail

output_tar="${1:?output tar path}"
work=/tmp/why3-web-build
src=/opt/why3-web
# shellcheck source=pins.env
source "${src}/pins.env"

mkdir -p "${work}"
cd "${work}"

echo "==> opam switch on the system OCaml"
export OPAMYES=1 OPAMCONFIRMLEVEL=unsafe-yes
opam init --disable-sandboxing --bare -y --no-setup
opam switch create default ocaml-system
eval "$(opam env --switch=default --set-switch)"

echo "==> OCaml libraries"
opam install -y dune dune-site menhir ocamlgraph zarith camlzip re ppxlib yojson ppx_deriving logs fmt \
  "js_of_ocaml.${JS_OF_OCAML_VERSION}" "js_of_ocaml-compiler.${JS_OF_OCAML_VERSION}" \
  "js_of_ocaml-ppx.${JS_OF_OCAML_VERSION}" "js_of_ocaml-lwt.${JS_OF_OCAML_VERSION}" \
  zarith_stubs_js data-encoding lwt_ppx

fetch() {
  local url="$1" sha="$2" out="$3"
  curl -sSL --retry 3 -o "${out}" "${url}"
  echo "${sha}  ${out}" | sha256sum -c -
}

echo "==> Why3 ${WHY3_VERSION}"
why3="${work}/why3"
git init -q "${why3}"
git -C "${why3}" fetch -q --depth 1 "${WHY3_GIT}" "${WHY3_COMMIT}"
git -C "${why3}" checkout -q --detach FETCH_HEAD
(
  cd "${why3}"
  ./autogen.sh
  ./configure --enable-local --disable-ide --disable-web-ide --disable-hypothesis-selection \
    --disable-doc --disable-emacs-compilation --disable-coq-libs --disable-pvs-libs \
    --disable-isabelle-libs --disable-java --disable-mpfr --disable-infer --disable-bddinfer --disable-sexp
  make -j"$(nproc)" byte plugins.byte
)

echo "==> Alt-Ergo ${ALT_ERGO_VERSION}"
fetch "${ALT_ERGO_URL}" "${ALT_ERGO_SHA256}" alt-ergo.tbz
tar -xjf alt-ergo.tbz
alt_ergo="${work}/alt-ergo-${ALT_ERGO_VERSION}"
(
  cd "${alt_ergo}"
  opam install -y --deps-only ./alt-ergo-lib.opam ./alt-ergo-parsers.opam ./alt-ergo.opam
  dune build --profile=release src/bin/js/worker_js.bc.js
)

echo "==> Creusot prelude (${CREUSOT_TAG})"
git init -q creusot
git -C creusot fetch -q --depth 1 https://github.com/creusot-rs/creusot.git "${CREUSOT_COMMIT}"
git -C creusot checkout -q --detach FETCH_HEAD -- prelude-generator LICENSE
node "${src}/gen-prelude.mjs" "${work}/creusot/prelude-generator" "${work}/prelude/creusot"

echo "==> proof worker"
mkdir -p worker
cp "${src}/worker/proof_worker.ml" worker/
(
  cd worker
  coma="${why3}/plugins/coma"
  ocamlfind ocamlc -g -I "${why3}/lib/why3" -I "${coma}" \
    -package menhirLib,re,unix,zarith,dynlink,zip,js_of_ocaml,yojson -linkpkg \
    "${why3}/lib/why3/why3.cma" \
    "${coma}/coma_logic.cmo" "${coma}/coma_syntax.cmo" "${coma}/coma_parser.cmo" \
    "${coma}/coma_lexer.cmo" "${coma}/coma_typing.cmo" "${coma}/coma_main.cmo" \
    proof_worker.ml -o proof_worker.byte
  files=()
  while IFS= read -r f; do
    files+=("--file=${f}:/share/stdlib/${f#"${why3}/stdlib/"}")
  done < <(find "${why3}/stdlib" -name "*.mlw" | sort)
  for f in "${work}"/prelude/creusot/*.coma; do
    files+=("--file=${f}:/packages/creusot/$(basename "${f}")")
  done
  js_of_ocaml --extern-fs \
    --file="${src}/why3.conf:/why3.conf" --file="${src}/try_alt_ergo.drv:/try_alt_ergo.drv" \
    "${files[@]}" \
    +dynlink.js +toplevel.js +zarith_stubs_js/runtime.js \
    proof_worker.byte -o proof_worker.js
)

echo "==> bundle"
bundle="${work}/bundle"
rm -rf "${bundle}"
mkdir -p "${bundle}/LICENSES"
cp worker/proof_worker.js "${bundle}/proof_worker.js"
cp "${alt_ergo}/_build/default/src/bin/js/worker_js.bc.js" "${bundle}/alt-ergo-worker.js"
cp "${why3}/LICENSE" "${bundle}/LICENSES/why3.LICENSE"
cp "${alt_ergo}/LICENSE.md" "${bundle}/LICENSES/alt-ergo.LICENSE.md"
cp -r "${alt_ergo}/licenses" "${bundle}/LICENSES/alt-ergo-licenses"
cp "${work}/creusot/LICENSE" "${bundle}/LICENSES/creusot.LICENSE"
cat > "${bundle}/manifest.json" <<JSON
{
  "why3": "${WHY3_VERSION}",
  "alt_ergo": "${ALT_ERGO_VERSION}",
  "creusot": "${CREUSOT_TAG}",
  "js_of_ocaml": "${JS_OF_OCAML_VERSION}",
  "built": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
JSON
tar -cf "${output_tar}" -C "${bundle}" .
ls -la "${bundle}"
echo "==> wrote ${output_tar}"
