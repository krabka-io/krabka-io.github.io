# why3-web

Why3 and Alt-Ergo compiled to JavaScript, plus Z3, cvc5 and CVC4 in WebAssembly, for the proof explorer's browser
re-check at `/docs/proof-explorer`.

The broker proves each `krabka-verified` kernel with Creusot, and why3find
records the session: which tactic split each verification condition and which
prover discharged each leaf. The explorer shows those sessions. With this
bundle present it can also replay one in the browser: Why3 loads the Coma file
Creusot generated, splits it into the recorded conditions, applies the recorded
tactics, and the recorded prover discharges each supported leaf.

## Contents of the bundle

| File | What it is |
| --- | --- |
| `proof_worker.js` | Why3 as a web worker (`worker/proof_worker.ml`), with Why3's stdlib, the Creusot prelude, `why3.conf` and the prover drivers embedded |
| `alt-ergo-worker.js` | Alt-Ergo over its Dolmen solving loop (`alt-ergo/ae_worker.ml`) |
| `smt-worker.js` | Z3, cvc5 and CVC4 adapter; one fresh WASM runtime per leaf |
| `z3-built.js`, `z3-built.wasm`, `z3-api.js` | the official `z3-solver` browser distribution and low-level API |
| `cvc5.js`, `cvc5.wasm` | the official cvc5 Wasm release; its JS environment flags and guard are adapted for a worker |
| `cvc4.js`, `cvc4.wasm` | CVC4 1.8 compiled from source with Emscripten; a modular CLI with its filesystem and entry point exposed |
| `manifest.json` | the versions below and the build time; the page probes it to learn whether the bundle is present |
| `LICENSES/` | Why3 (LGPL 2.1), Alt-Ergo (see its licence files) and Creusot (LGPL 2.1), Z3 (MIT) and cvc5/CVC4 with their bundled third-party licences |

## Pins

`pins.env` names every input: the Why3 and Alt-Ergo releases that Creusot
v0.13.0's `creusot-setup` installs on the proof machine, and the Creusot commit
whose `prelude-generator` produces the Why3 prelude the Coma files reference.
Z3 4.15.3 and cvc5 1.3.1 match Creusot's native solver pins; their official
browser archives and cvc5's source licence archive are SHA-256 checked.
CVC4 1.8 is compiled by `build-cvc4-web.sh` with Emscripten 3.1.74, GMP,
ANTLR 3.4 and the SymFPU revision its release recommends. All source/toolchain
archives are SHA-256 checked. Build scripts are adapted to use Python's
built-in TOML parser and to preserve literal ampersands in modern Bash
code generation, and three headers explicitly include `stddef.h` for libc++.
ANTLR's unused debug handlers are omitted as in CVC4's
own dependency build script. Native signal-stack/resource initialization is
skipped in WebAssembly; the page enforces its budget by terminating the worker.
`gen-prelude.mjs` is a port of that generator, so the builder needs no Rust.

## Build

Bazel drives it, the way krabka-broker builds its Creusot proof image:

```sh
bazel build //why3-web:bundle        # bazel-bin/why3-web/why3-web.tar
bash why3-web/install.sh             # npm run build:why3-web: unpack into public/why3-web/
npm run check-proof-workers          # Why3 drivers and Alt-Ergo in browser globals
npm run build:site
npx playwright install chromium
npm run check-proof-smt              # real WASM workers and page replay in Chromium
```

`image.apko.yaml` and its lock are the Wolfi base with opam, a C toolchain and
Node, Python, CMake and Java for CVC4; opam builds the OCaml compiler pinned in `pins.env` inside the container,
because js_of_ocaml 6.2.0 does not accept the newest OCaml that Wolfi ships.
Bazel assembles the builder image from that base plus the scripts and
worker source here, loads it into Docker, runs `build-why3-web.sh` inside it,
and copies the tar out. The action needs the network (opam, the pinned source
archives) and Docker, so it is `local` and unsandboxed; its inputs are the
locked base, `pins.env`, the scripts and the worker, so Bazel reuses the cached
tar until one of them changes.

Regenerate the lock after editing `image.apko.yaml`:

```sh
apko lock why3-web/image.apko.yaml
```

Without Docker (`install.sh` checks), the site builds without the bundle and
the explorer says the browser re-check is not part of the build.

## Worker protocol

The page and `proof_worker.js` exchange JSON strings:

| Request | Reply |
| --- | --- |
| `{"cmd":"ping"}` | `{"kind":"pong","why3":..,"prover":..}` |
| `{"cmd":"load","name":..,"content":<coma>}` | `{"kind":"loaded","theories":[{"name":..,"goals":[{"id":..,"name":"vc_..","expl":..}]}]}` |
| `{"cmd":"transform","id":..,"name":"split_vc"}` | `{"kind":"children","id":..,"children":[{"id":..,"expl":..}]}` |
| `{"cmd":"task","id":..,"prover":"alt-ergo"|"z3"|"cvc5"|"cvc4"}` | `{"kind":"task","id":..,"text":<SMT-LIB task>,"pretty":<sequent>}` |

Any failure answers `{"kind":"error",...}`.

`alt-ergo-worker.js` is `alt-ergo/ae_worker.ml`, a small worker over
Alt-Ergo's own solving loop, so the Dolmen front end and SMT-LIB input work
in the browser (Alt-Ergo's shipped `worker_js` carries only the legacy front
end). It takes `{"id":..,"filename":"task.smt2","content":..,"steps":..}` and
answers `{"id":..,"status":"unsat"|"sat"|"unknown"|"timeout"|"error","output":..,"diagnostic":..,"ms":..}`;
`unsat` means proved. `ae_worker_stubs.js` supplies the Unix timer primitives
the loop touches, which a worker has no use for; the page enforces its own
time limit by terminating a worker.

## Notes from building it

- Creusot 0.13.0's Coma files use `[%#span]` references that the Why3 1.8.2
  release does not parse; the pinned commit does, and it builds with dune, so
  the worker is a dune executable copied into the checkout
  (`worker/dune`).
- The worker is linked with `js_of_ocaml --effects=cps`: Why3's reduction
  engine (`compute_specified`) recurses deeply on a large verification
  condition and overflows the JavaScript stack otherwise.
- Alt-Ergo disables js_of_ocaml 6.2 inlining, which otherwise moves part of
  `Satml_types.mk_and` outside its `Exit` handler and fails valid goals with
  `Function 'exit' not implemented`. The worker check exercises both bundles,
  solver errors, the step budget and reuse after failures.
- The Why3 side prints tasks with Why3's Try Why3 driver for Alt-Ergo's
  Dolmen front end (`try_alt_ergo.drv`, SMT-LIB with polymorphic
  declarations); the Alt-Ergo worker reads them as the `psmt2` dialect. CI
  proves through Why3's `alt_ergo_26` SMT-LIB driver: same prover, same
  verification condition, same input language.

Z3, cvc5 and CVC4 use Why3's pinned `z3_4_12.drv`, `cvc5.drv` and
`cvc4_17.drv` (Why3's driver for CVC4 1.8), including the
upstream driver imports embedded in the Why3 worker. Omitting `prover` keeps
the Alt-Ergo driver for existing clients.

The SMT worker accepts `{id, content}` JSON strings and returns the same
`{id, status, output, diagnostic, ms}` protocol as Alt-Ergo. Only one exact
`unsat` answer without solver errors counts as proved. Each runtime is
terminated after the leaf, on cancellation, or after the page's 60-second
limit. The page caps each prover at two workers to bound WASM memory use.

Z3's upstream build requires shared memory. The page reuses the Cluster Lab's
isolation helper with a service worker scoped only to `/docs/proof-explorer/`.
The first Z3 check reloads once and resumes the selected session; hosts that
already send COOP/COEP headers need no reload. If isolation cannot be enabled,
the panel reports why. Blob worker bootstraps inherit the page's isolation
policy even though the bundle lives outside the service worker's scope; Z3's
pthread workers use the same approach. The cvc5, CVC4 and Alt-Ergo workers need no
isolation.
