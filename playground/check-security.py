#!/usr/bin/env python3
"""Reject vulnerable ruzstd resolutions in the shipped WASM target graph."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parent
result = subprocess.run(
    ["cargo", "tree", "--locked", "--target", "wasm32-unknown-unknown",
     "--prefix", "none", "--format", "{p}", "-i", "ruzstd"],
    cwd=root, check=True, capture_output=True, text=True,
)
print(result.stdout, end="")
versions = re.findall(r"^ruzstd v(\S+)", result.stdout, re.MULTILINE)
if not versions:
    raise SystemExit("Expected ruzstd in the WASM graph; review the security guard")
for version in versions:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", version)
    if not match or tuple(map(int, match.groups())) < (0, 9, 1):
        raise SystemExit(f"Unpatched or unreviewed ruzstd version: {version}")
print("RUSTSEC-2026-0344: WASM target resolves patched ruzstd")
