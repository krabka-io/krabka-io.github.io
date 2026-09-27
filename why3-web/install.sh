#!/usr/bin/env bash
# `npm run build:why3-web`: build the bundle with Bazel and unpack it into
# public/why3-web/, where the proof explorer loads it from. A machine without
# Docker cannot build it; the explorer then says the browser re-check is not
# part of the build, and the rest of the site is unaffected.
set -euo pipefail

cd "$(dirname "$0")/.."
out="public/why3-web"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "why3-web: Docker is not available; skipping the browser re-check bundle." >&2
  exit 0
fi

bazel build //why3-web:bundle
rm -rf "${out}"
mkdir -p "${out}"
tar -xf bazel-bin/why3-web/why3-web.tar -C "${out}"
echo "why3-web: bundle unpacked into ${out}:"
ls -la "${out}"
