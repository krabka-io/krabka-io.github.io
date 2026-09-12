# krabka-playground

WebAssembly bindings that drive [Krabka](https://github.com/krabka-io)'s
deterministic `KRaft` consensus engine in the browser.

This crate is a thin `wasm-bindgen` shim over `krabka-kraft-core`'s `sim::Sim`.
`sim::Sim` is the pure, sans-IO multi-node simulator that the broker
integration tests drive. The shim powers the
[consensus playground](https://krabka-io.github.io/docs/playground) page. On
that page you can inject partitions, drop or reorder or duplicate messages, and
append records. You then watch a cluster elect a leader, lose it, and recover.
All of this is live, with no backend.

## Layout

| Path | What it holds |
| --- | --- |
| `playground/src/lib.rs` | The `wasm_bindgen` shim. |
| `playground/build.sh` | The build. It writes the WASM module into `public/playground/`. |
| `public/playground/app.js` | The hand-written front-end that drives the module. |
| `src/styles/playground.css` | The panel styles. Astro bundles them. |
| `src/pages/docs/playground.astro` | The page that mounts the panel. |

## Building

The site build runs this crate:

```sh
npm run build            # build:playground, then astro build
npm run build:playground # the WASM only
```

`build.sh` needs `cargo`. It adds the `wasm32-unknown-unknown` target through
`rustup` when `rustup` is present, and it downloads the `wasm-bindgen` CLI whose
version matches `playground/Cargo.lock`. The generated
`krabka_playground.js` and `krabka_playground_bg.wasm` are git-ignored. The
build regenerates them, locally and in CI.

`npm run dev` does not build the WASM, so a documentation change needs no Rust
toolchain. Run `npm run build:playground` once if you want the panel live in the
dev server.

## Cross-repository dependencies

`krabka-kraft-core` lives in
[`krabka-io/krabka-broker`](https://github.com/krabka-io/krabka-broker) and is
not published to crates.io. `Cargo.toml` pins it by git tag. Its own unpublished
dependencies (`krabka-ids`, `krabka-units`, `krabka-voters`) come from
`krabka-io/krabka-protocol` through `[patch.crates-io]`, which is the same
mechanism the broker workspace uses. Bump the tag and the revision together.

## Licence

Apache License 2.0.
