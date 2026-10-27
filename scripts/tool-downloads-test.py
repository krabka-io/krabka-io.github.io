"""Adversarial fixtures; no network or downloaded executable is run."""
import hashlib
import importlib.util
import io
import json
import os
import sys
import subprocess
import select
from pathlib import Path, PureWindowsPath
import tarfile
import tempfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('installer', Path(__file__).with_name('install-tool.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

temp_parent = Path(os.environ.get('TMPDIR', Path(__file__).resolve().parents[1] / 'playground/.tools'))
temp_parent.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix='tool-tests-', dir=temp_parent) as temporary:
    root = Path(temporary)
    manifest = root / 'pins.json'
    m.PINS = manifest

    def fixture(entries):
        content = io.BytesIO()
        with tarfile.open(fileobj=content, mode='w:gz') as archive:
            for name, target in entries:
                member = tarfile.TarInfo(name)
                if target is not None:
                    member.type = tarfile.SYMTYPE
                    member.linkname = target
                    archive.addfile(member)
                else:
                    member.mode = 0o755
                    member.size = 7
                    archive.addfile(member, io.BytesIO(b'fixture'))
        return content.getvalue()

    def configure(data, sha=None, archive_root='tool'):
        checksum = sha or hashlib.sha256(data).hexdigest()
        pin = {'version': '1', 'platforms': {'test': {'url': 'https://example.invalid/tool', 'sha256': checksum, 'archive_root': archive_root, 'executable': 'run'}}}
        manifest.write_text(json.dumps({'kind': pin}))
        return checksum

    def response(data):
        stream = io.BytesIO(data)
        stream.url = 'https://example.invalid/tool'
        return stream

    def rejects(action):
        try:
            action()
        except (ValueError, KeyError, OSError):
            return
        raise AssertionError('expected fail-closed rejection')

    good = fixture([('tool/run', None), ('tool/link', 'run'), ('tool/self', '.'), ('tool/sub/parent-link', '../run')])
    configure(good)
    with patch.object(m.urllib.request, 'urlopen', return_value=response(good)):
        installed = m.install('kind', cache=root / 'cache', host='test')
    assert installed.read_bytes() == b'fixture'
    installed.write_text('tampered')
    with patch.object(m.urllib.request, 'urlopen', side_effect=AssertionError('cache should avoid network')):
        restored = m.install('kind', cache=root / 'cache', host='test')
    assert restored != installed and restored.read_bytes() == b'fixture'
    next((root / 'cache').glob('*.download')).write_text('tampered archive')
    with patch.object(m, 'extract', side_effect=AssertionError('must not extract')):
        rejects(lambda: m.install('kind', cache=root / 'cache', host='test'))
    configure(good, '0' * 64)
    with patch.object(m.urllib.request, 'urlopen', return_value=response(good)), patch.object(m, 'extract', side_effect=AssertionError('must not extract')):
        rejects(lambda: m.install('kind', cache=root / 'bad-checksum', host='test'))
    assert not list((root / 'bad-checksum').glob('*'))
    configure(good)
    with patch.object(m.urllib.request, 'urlopen', side_effect=OSError('interrupted')):
        rejects(lambda: m.install('kind', cache=root / 'interrupted', host='test'))
    assert not list((root / 'interrupted').glob('*'))
    rejects(lambda: m.install('kind', version='2', cache=root / 'unused', host='test'))
    rejects(lambda: m.install('kind', cache=root / 'unused', host='unknown'))
    configure(good, '')
    pin = json.loads(manifest.read_text()); del pin['kind']['platforms']['test']['sha256']; manifest.write_text(json.dumps(pin))
    rejects(lambda: m.install('kind', cache=root / 'unused', host='test'))
    for entries in [
        [('tool/run', None), ('../escaped', None)],
        [('tool/run', None), ('/absolute', None)],
        [('tool/run', None), ('tool/link', '../../escaped')],
        [('tool/link', 'safe'), ('tool/link/run', None)],
        [('tool/run', None), ('tool/run', None)],
        [('tool/run', None), ('tool/C:\\escaped', None)],
        [('tool/a', '.'), ('tool/sub/b', '../a/../outside')],
        [('tool/a', 'b'), ('tool/b', 'a')],
        [('tool/.. /escaped', None)],
        [('tool/name.', None)],
        [('tool/NUL', None)],
        [('tool/CON.txt', None)],
        [('tool/aux', None)],
        [('tool/PRN', None)],
        [('tool/COM1.log', None)],
        [('tool/LPT9', None)],
        [('tool/link', '.. /escaped')],
        [('tool/link', 'name.')],
        [('tool/link', '../CON.txt')],
    ]:
        data = fixture(entries)
        archive = root / 'unsafe.tar.gz'; archive.write_bytes(data)
        destination = root / 'extract'; destination.mkdir(exist_ok=True)
        rejects(lambda: m.extract(archive, destination, 'tool'))
        assert not list(destination.iterdir()), 'all headers must be checked before extraction'
    # Legacy install directories must not be adopted or executed.
    legacy = root / 'legacy'; legacy.mkdir(); (legacy / 'kind').write_text('unverified')
    configure(good)
    with patch.object(m.urllib.request, 'urlopen', return_value=response(good)):
        assert m.install('kind', cache=legacy, host='test').read_bytes() == b'fixture'

    # Stable SDK paths still reconstruct from verified bytes on every use.
    checksum = configure(good)
    sdk_pin = json.loads(manifest.read_text())['kind']
    sdk_pin['platforms'][m.host_platform()] = sdk_pin['platforms'].pop('test')
    manifest.write_text(json.dumps({'wasi-sdk': sdk_pin}))
    sdk_cache = root / 'sdk-cache'
    with patch.object(m.urllib.request, 'urlopen', return_value=response(good)):
        sdk = m.install('wasi-sdk', cache=sdk_cache)
    assert sdk.parent.name == f'wasi-sdk-1-{m.host_platform()}-{checksum}'
    (sdk / 'run').write_text('tampered')
    sibling = sdk_cache / 'unrelated'; sibling.mkdir(); (sibling / 'keep').write_text('keep')
    with patch.object(m.urllib.request, 'urlopen', side_effect=AssertionError('no network')):
        assert m.install('wasi-sdk', cache=sdk_cache) == sdk
    assert (sdk / 'run').read_bytes() == b'fixture'
    assert (sibling / 'keep').read_text() == 'keep'

    # Exercise the actual CLI and Bash inherited-descriptor lifetime, not a
    # model of flock. The first shell owns the SDK through its exit cleanup.
    cli = root / 'install-tool.py'; cli.write_text(Path(m.__file__).read_text())
    (root / 'tool-pins.json').write_text(manifest.read_text())
    script = '''set -euo pipefail
lock="$(python3 "$1" wasi-sdk --cache "$2" --lock-path)"
exec {sdk_fd}>"$lock"
sdk="$(python3 "$1" wasi-sdk --cache "$2" --lock-fd "$sdk_fd")"
owned="$(dirname "$sdk")"
trap 'rm -rf -- "$owned"' EXIT
echo ready
read -r release
test -s "$sdk/run"
'''
    args = ['bash', '-c', script, 'sdk-build', str(cli), str(sdk_cache)]
    first = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    second = None
    try:
        assert select.select([first.stdout], [], [], 10)[0], 'first caller did not start'
        assert first.stdout.readline().strip() == 'ready'
        second = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert not select.select([second.stdout], [], [], 0.3)[0], 'second caller bypassed ownership'
        assert (sdk / 'run').is_file(), 'waiting caller must not clear an in-use SDK'
        first.stdin.write('release\n'); first.stdin.flush()
        assert first.wait(timeout=10) == 0, first.stderr.read()
        assert select.select([second.stdout], [], [], 10)[0], 'lock was not released'
        assert second.stdout.readline().strip() == 'ready'
        assert (sdk / 'run').read_bytes() == b'fixture', 'next caller reconstructs after cleanup'
        second.stdin.write('release\n'); second.stdin.flush()
        assert second.wait(timeout=10) == 0, second.stderr.read()
        assert not sdk.parent.exists()
        assert (sibling / 'keep').read_text() == 'keep'
        assert list(sdk_cache.glob('*.download')) and list(sdk_cache.glob('*.lock'))
    finally:
        for process in [first, second]:
            if process is not None and process.poll() is None:
                process.kill(); process.wait(timeout=10)
    next(sdk_cache.glob('*.download')).write_text('corrupt')
    with patch.object(m, 'extract', side_effect=AssertionError('must not extract')):
        rejects(lambda: m.install('wasi-sdk', cache=sdk_cache))
# Git Bash callers need forward slashes for dirname and cleanup paths.
with patch.object(sys, 'argv', ['install-tool.py', 'kind']), patch.object(m, 'install', return_value=PureWindowsPath('C:/tools/kind.exe')), patch('sys.stdout', new_callable=io.StringIO) as output:
    m.main()
    assert output.getvalue() == 'C:/tools/kind.exe\n'
print('verified download and archive rejection fixtures passed')
