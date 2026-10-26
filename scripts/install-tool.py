#!/usr/bin/env python3
"""Install only repository-pinned tools. Diagnostics use stderr; stdout is a path."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import platform
import shutil
import sys
import tarfile
import tempfile
import urllib.request

PINS = Path(__file__).with_name('tool-pins.json')


def host_platform():
    system = platform.system().lower()
    if system.startswith(('mingw', 'msys', 'cygwin')):
        system = 'windows'
    machine = {'amd64': 'x86_64', 'aarch64': 'arm64'}.get(platform.machine().lower(), platform.machine().lower())
    return f'{system}-{machine}'


def digest(path):
    with open(path, 'rb') as source:
        checksum = hashlib.sha256()
        for block in iter(lambda: source.read(1024 * 1024), b''):
            checksum.update(block)
        return checksum.hexdigest()


def portable_components(name, allow_parent=False):
    # Win32 normalizes trailing spaces/dots and treats device names specially,
    # even when a POSIX tar reader sees an ordinary component.
    for component in name.split('/'):
        if component in ('', '.') or (allow_parent and component == '..'):
            continue
        if component.endswith((' ', '.')) or PureWindowsPath(component).is_reserved():
            raise ValueError(f'nonportable archive path: {name}')


def inside(name, root):
    # Backslashes and drive names can escape on Windows even in POSIX archives.
    portable_components(name)
    path = PurePosixPath(name)
    if '\\' in name or ':' in name or path.is_absolute() or '..' in path.parts:
        raise ValueError(f'unsafe archive path: {name}')
    if not path.parts or path.parts[0] != root:
        raise ValueError(f'unexpected archive root: {name}')
    return path


def extract(archive, destination, root):
    with tarfile.open(archive, 'r:gz') as source:
        members = source.getmembers()
        names = set()
        links = {}
        for member in members:
            path = inside(member.name, root)
            if str(path) in names:
                raise ValueError(f'duplicate archive path: {path}')
            names.add(str(path))
            if member.issym():
                target = member.linkname
                portable_components(target, allow_parent=True)
                if '\\' in target or ':' in target or target.startswith('/'):
                    raise ValueError(f'unsafe archive link: {path}')
                resolved = list(path.parent.parts)
                for part in PurePosixPath(target).parts:
                    if part == '..':
                        if len(resolved) <= 1:
                            raise ValueError(f'escaping archive link: {path}')
                        resolved.pop()
                    elif part != '.':
                        resolved.append(part)
                links[str(path)] = target
            elif not (member.isfile() or member.isdir()):
                raise ValueError(f'unsupported archive member: {path}')
        # Resolve links in a virtual archive tree, including intermediate
        # symlinks BEFORE '..'. Lexical normalization alone misses escapes.
        for link, target in links.items():
            pending = list(PurePosixPath(link).parent.parts) + list(PurePosixPath(target).parts)
            resolved = []
            expansions = 0
            while pending:
                part = pending.pop(0)
                if part == '..':
                    if len(resolved) <= 1:
                        raise ValueError(f'escaping chained archive link: {link}')
                    resolved.pop()
                elif part != '.':
                    resolved.append(part)
                    candidate = '/'.join(resolved)
                    if candidate in links:
                        expansions += 1
                        if expansions > 40:
                            raise ValueError(f'cyclic archive link: {link}')
                        resolved.pop()
                        pending = list(PurePosixPath(links[candidate]).parts) + pending
        for member in members:
            path = inside(member.name, root)
            if any(str(parent) in links for parent in path.parents):
                raise ValueError(f'archive member below symlink: {path}')
        # All headers are validated before any content is written. Create links last.
        for member in sorted(members, key=lambda item: item.issym()):
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.issym():
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(member.linkname)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as incoming, open(target, 'xb') as outgoing:
                    shutil.copyfileobj(incoming, outgoing)
                target.chmod(member.mode & 0o777)


def install(tool, version=None, cache=None, host=None):
    pins = json.loads(PINS.read_text())
    entry = pins[tool]
    host = host or host_platform()
    if version and version != entry['version']:
        raise ValueError(f'{tool} {version} is not pinned; update scripts/tool-pins.json (see docs/tool-downloads.md)')
    if host not in entry['platforms']:
        raise ValueError(f'unsupported {tool} platform: {host}; use an explicit local toolchain override')
    pin = entry['platforms'][host]
    sha = pin['sha256']
    if len(sha) != 64 or any(c not in '0123456789abcdef' for c in sha):
        raise ValueError('missing or invalid SHA-256 pin')
    if not pin['url'].startswith('https://'):
        raise ValueError('tool download requires HTTPS')
    cache = Path(cache or Path(__file__).resolve().parents[1] / 'playground/.tools/verified').resolve()
    cache.mkdir(parents=True, exist_ok=True)
    artifact = cache / f'{tool}-{entry["version"]}-{host}-{sha}.download'
    if artifact.exists() and (artifact.is_symlink() or digest(artifact) != sha):
        raise ValueError(f'cached download checksum mismatch: {artifact}; remove it and retry')
    if not artifact.exists():
        with tempfile.TemporaryDirectory(prefix='download-', dir=cache) as work:
            incoming = Path(work) / 'artifact'
            print(f'Downloading {pin["url"]}', file=sys.stderr)
            with urllib.request.urlopen(pin['url'], timeout=120) as response, open(incoming, 'wb') as output:
                if not response.url.startswith('https://'):
                    raise ValueError('download redirected away from HTTPS')
                shutil.copyfileobj(response, output)
            if digest(incoming) != sha:
                raise ValueError(f'{tool} download checksum mismatch; nothing installed')
            os.replace(incoming, artifact)
    # Reconstruct from the verified archive on EVERY use: installed files and
    # marker files are not trusted. Each caller owns its isolated installation.
    work = Path(tempfile.mkdtemp(prefix=f'{tool}-install-', dir=cache))
    try:
        if pin['archive_root']:
            extract(artifact, work, pin['archive_root'])
            installed = work / pin['archive_root']
        else:
            installed = work
            shutil.copyfile(artifact, installed / pin['executable'])
            (installed / pin['executable']).chmod(0o755)
        executable = installed / pin['executable']
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ValueError(f'missing executable: {pin["executable"]}')
        return installed if tool == 'wasi-sdk' else executable
    except BaseException:
        shutil.rmtree(work)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('tool', choices=['wasm-bindgen', 'wasi-sdk', 'kind'])
    parser.add_argument('--version')
    parser.add_argument('--cache')
    args = parser.parse_args()
    print(install(args.tool, args.version, args.cache).as_posix())


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, OSError, tarfile.TarError) as error:
        sys.exit(f'error: {error}')
