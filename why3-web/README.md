# why3-web

Why3 and Alt-Ergo compiled to JavaScript, for the proof explorer's browser
re-check at `/docs/proof-explorer`.

The broker proves each `krabka-verified` kernel with Creusot, and why3find
records the session: which tactic split each verification condition and which
prover discharged each leaf. The explorer shows those sessions. With this
bundle present it can also replay one in the browser: Why3 loads the Coma file
Creusot generated, splits it into the recorded conditions, applies the recorded
tactics, and Alt-Ergo, compiled to JavaScript, discharges each leaf.

## Contents of the bundle

| File | What it is |
| --- | --- |
| `proof_worker.js` | Why3 as a web worker (`worker/proof_worker.ml`), with Why3's stdlib, the Creusot prelude, `why3.conf` and the Alt-Ergo driver embedded |
| `alt-ergo-worker.js` | Alt-Ergo's own web worker (`src/bin/js/worker_js.ml` in the Alt-Ergo release) |
| `manifest.json` | the versions below and the build time; the page probes it to learn whether the bundle is present |
| `LICENSES/` | Why3 (LGPL 2.1), Alt-Ergo (see its licence files) and Creusot (LGPL 2.1) |

## Pins

`pins.env` names every input: the Why3 and Alt-Ergo releases that Creusot
v0.13.0's `creusot-setup` installs on the proof machine, and the Creusot commit
whose `prelude-generator` produces the Why3 prelude the Coma files reference.
`gen-prelude.mjs` is a port of that generator, so the builder needs no Rust.

## Build

Bazel drives it, the way krabka-broker builds its Creusot proof image:

```sh
bazel build //why3-web:bundle        # bazel-bin/why3-web/why3-web.tar
bash why3-web/install.sh             # npm run build:why3-web: unpack into public/why3-web/
```

`image.apko.yaml` and its lock are the Wolfi base with OCaml 5.3, opam, dune
and Node. Bazel assembles the builder image from that base plus the scripts and
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
| `{"cmd":"task","id":..}` | `{"kind":"task","id":..,"text":<prover input>,"pretty":<sequent>}` |

Any failure answers `{"kind":"error",...}`. The Alt-Ergo worker takes
`[0, inputJson, optionsJson]` (a js_of_ocaml pair) and answers Alt-Ergo's
`results` record as JSON; `{"Unsat": steps}` in `status` means proved.

The browser re-check prints tasks with Why3's native Alt-Ergo driver
(`try_alt_ergo.drv`, from Why3's Try Why3), because Alt-Ergo's JavaScript
worker only carries its legacy front end. CI proves through Why3's SMT-LIB
driver for Alt-Ergo 2.6. Same prover, same verification condition, different
printer.
