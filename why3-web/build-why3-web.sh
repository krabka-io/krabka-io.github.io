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

echo "==> opam switch on OCaml ${OCAML_VERSION}"
export OPAMYES=1 OPAMCONFIRMLEVEL=unsafe-yes
opam init --disable-sandboxing --bare -y --no-setup
opam switch create default "ocaml-base-compiler.${OCAML_VERSION}"
eval "$(opam env --switch=default --set-switch)"

echo "==> OCaml libraries"
# Everything Why3, Alt-Ergo and the two workers link against, in one solve.
# Alt-Ergo's opam files are not consulted: their upper bounds on cmdliner and
# ppxlib predate the versions the worker was verified with, and honouring
# them would downgrade and rebuild half the switch for nothing.
opam install -y dune dune-site dune-build-info menhir ocamlgraph zarith camlzip re ppxlib yojson \
  ppx_deriving ppx_blob logs fmt seq result stdlib-shims cmdliner data-encoding lwt_ppx \
  dolmen dolmen_type dolmen_loop ocplib-simplex psmt2-frontend \
  "js_of_ocaml.${JS_OF_OCAML_VERSION}" "js_of_ocaml-compiler.${JS_OF_OCAML_VERSION}" \
  "js_of_ocaml-ppx.${JS_OF_OCAML_VERSION}" "js_of_ocaml-lwt.${JS_OF_OCAML_VERSION}" zarith_stubs_js

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
mkdir -p "${why3}/src/proof_worker"
cp "${src}/worker/proof_worker.ml" "${src}/worker/dune" "${why3}/src/proof_worker/"
(
  cd "${why3}"
  ./autogen.sh
  ./configure --enable-local --disable-ide --disable-web-ide --disable-hypothesis-selection \
    --disable-doc --disable-emacs-compilation --disable-coq-libs --disable-pvs-libs \
    --disable-isabelle-libs --disable-java --disable-mpfr --disable-infer --disable-bddinfer --disable-sexp
  # Two modules the Makefile generates before it calls dune: the install
  # paths from configure and the parser's handcrafted error messages.
  make src/util/config.ml src/parser/parser_messages.ml
  dune build src/proof_worker/proof_worker.bc
)

echo "==> Alt-Ergo ${ALT_ERGO_VERSION}"
fetch "${ALT_ERGO_URL}" "${ALT_ERGO_SHA256}" alt-ergo.tbz
bzip2 -dc alt-ergo.tbz | tar -xf -
alt_ergo="${work}/alt-ergo-${ALT_ERGO_VERSION}"
cp "${src}/alt-ergo/ae_worker.ml" "${src}/alt-ergo/ae_worker_stubs.js" "${alt_ergo}/src/bin/js/"
cat "${src}/alt-ergo/dune.stanza" >> "${alt_ergo}/src/bin/js/dune"
(
  cd "${alt_ergo}"
  dune build --profile=release src/bin/js/ae_worker.bc.js
)

echo "==> Creusot prelude (${CREUSOT_TAG})"
git init -q creusot
git -C creusot fetch -q --depth 1 https://github.com/creusot-rs/creusot.git "${CREUSOT_COMMIT}"
git -C creusot checkout -q FETCH_HEAD -- prelude-generator LICENSE
node "${src}/gen-prelude.mjs" "${work}/creusot/prelude-generator" "${work}/prelude/creusot"

echo "==> proof worker"
# CPS mode: Why3's reduction engine recurses deeply on a large verification
# condition, and only js_of_ocaml's CPS translation keeps that off the
# JavaScript stack.
files=()
while IFS= read -r f; do
  files+=("--file=${f}:/share/stdlib/${f#"${why3}/stdlib/"}")
done < <(find "${why3}/stdlib" -name "*.mlw" -o -name "*.coma" | sort)
for f in "${work}"/prelude/creusot/*.coma; do
  files+=("--file=${f}:/packages/creusot/$(basename "${f}")")
done
mkdir -p worker
js_of_ocaml --effects=cps --extern-fs \
  --file="${src}/why3.conf:/why3.conf" --file="${src}/try_alt_ergo.drv:/try_alt_ergo.drv" \
  "${files[@]}" \
  +dynlink.js +toplevel.js +zarith_stubs_js/runtime.js \
  "${why3}/_build/default/src/proof_worker/proof_worker.bc" -o worker/proof_worker.js

echo "==> bundle"
bundle="${work}/bundle"
rm -rf "${bundle}"
mkdir -p "${bundle}/LICENSES"
cp worker/proof_worker.js "${bundle}/proof_worker.js"
cp "${alt_ergo}/_build/default/src/bin/js/ae_worker.bc.js" "${bundle}/alt-ergo-worker.js"
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
