"""Validate and unpack the complete queue site into a clean staging directory."""
import json
from pathlib import Path, PurePosixPath
import sys
import tarfile


def unpack(archive, destination):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    with tarfile.open(archive, 'r:') as source:
        members = source.getmembers()
        names = set()
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or '..' in path.parts or '\\' in member.name or ':' in member.name:
                raise ValueError('unsafe queue site path')
            if not (member.isdir() or member.isfile()) or str(path) in names:
                raise ValueError('unsupported or duplicate queue site member')
            names.add(str(path))
        required = ['index.html', 'sitemap-index.xml', 'sitemap-inventory.json',
                    'playground/krabka_playground.js', 'playground/krabka_playground_bg.wasm',
                    'playground/broker/krabka-broker.wasm', 'why3-web/manifest.json']
        for name in required:
            if not any(str(PurePosixPath(member.name)) == name and member.isfile() and member.size > 0 for member in members):
                raise ValueError(f'incomplete queue site: {name}')
        # Reject all links, including hard links, before writing any file.
        for member in members:
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as incoming, open(target, 'xb') as outgoing:
                    import shutil
                    shutil.copyfileobj(incoming, outgoing)
    json.loads((destination / 'sitemap-inventory.json').read_text())
    json.loads((destination / 'why3-web/manifest.json').read_text())


if __name__ == '__main__':
    unpack(sys.argv[1], sys.argv[2])
