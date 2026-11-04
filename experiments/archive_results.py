"""Lossless external backup/restore of experiment outputs, including independent V1 oracles.

The archive is local data, never a Git artifact. Inputs are never removed or overwritten.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
import zipfile
from pathlib import Path, PurePosixPath


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def safe_path(name):
    p = PurePosixPath(name)
    if not name or p.is_absolute() or '..' in p.parts or '\\' in name or ':' in name:
        raise ValueError(f'unsafe member: {name}')
    return p


def inventory(repo):
    repo = Path(repo).resolve()
    entries = {}

    def add(path, member, expected=None):
        path = Path(path).resolve()
        safe_path(member)
        actual = sha(path)
        if expected and actual != expected:
            raise ValueError(f'provenance SHA mismatch: {path}')
        if member in entries:
            raise ValueError(f'duplicate member: {member}')
        entries[member] = {'path': member, 'original_path': str(path),
                           'bytes': path.stat().st_size, 'sha256': actual}

    # All local results, including ignored logs, raw samples and run executables.
    roots = [repo/'experiments/artifacts', repo/'.cache/independent-golden']
    roots.extend(p for p in (repo/'.cache').iterdir() if p.is_dir()
                 and ('plots' in p.name or p.name in ('samples-roundtrip', 'archive-compact-plots-check')))
    for root in roots:
        for path in sorted(root.rglob('*')):
            if path.is_file():
                add(path, 'repo/'+path.relative_to(repo).as_posix())
    # Local audit outputs are data too; exclude source/database/build caches.
    for path in sorted((repo/'.cache').iterdir()):
        if path.is_file() and path.suffix in ('.log', '.json', '.png', '.csv', '.txt', '.env', '.ps1', '.py'):
            add(path, 'repo/'+path.relative_to(repo).as_posix())
    saved = json.loads((repo/'experiments/artifacts/golden-manifest.json').read_text(encoding='utf-8'))
    for entry in saved['files']:
        source = Path(entry['path'].replace('\\\\', '\\'))
        add(source, 'external/historical-golden/'+entry['relative_path'], entry['sha256'])
    generated = json.loads((repo/'experiments/artifacts/v1-generated-golden.json').read_text(encoding='utf-8'))
    for run in generated:
        for entry in run['files']:
            source = Path(entry['path'].replace('\\\\', '\\')).resolve()
            member = 'repo/'+source.relative_to(repo).as_posix()
            if entries[member]['sha256'] != entry['sha256']:
                raise ValueError(f'generated V1 SHA mismatch: {source}')
    # Retain the old backup byte for byte, in addition to its uncompressed samples.
    locator = json.loads((repo/'experiments/artifacts/raw-samples-manifest.json').read_text(encoding='utf-8'))
    previous = Path(locator['external_manifest_path'])
    from archive_samples import verify_archive
    verify_archive(previous, source=repo)
    if sha(previous) != locator['external_manifest_sha256']:
        raise ValueError('previous manifest SHA mismatch')
    for path in sorted(previous.parent.iterdir()):
        if path.is_file():
            add(path, 'previous-archives/'+previous.parent.name+'/'+path.name)
    return sorted(entries.values(), key=lambda e: e['path'])


def create(directory, entries, commit):
    directory = Path(directory).resolve()
    if not entries:
        raise ValueError('empty inventory')
    directory.mkdir(parents=True, exist_ok=False)
    archive = directory/'experiment-results.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=1, allowZip64=True) as out:
        for entry in entries:
            safe_path(entry['path'])
            out.write(entry['original_path'], entry['path'])
    result = {'schema_version': 1, 'source_commit': commit,
              'archive': archive.name, 'archive_bytes': archive.stat().st_size,
              'archive_sha256': sha(archive), 'file_count': len(entries),
              'uncompressed_bytes': sum(e['bytes'] for e in entries), 'files': entries}
    manifest = directory/'manifest.json'
    manifest.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8', newline='\n')
    (directory/'SHA256SUMS').write_text(
        f'{sha(archive)}  {archive.name}\n{sha(manifest)}  {manifest.name}\n', encoding='ascii', newline='\n')
    verify(manifest)
    return manifest


def verify(manifest):
    manifest = Path(manifest).resolve()
    data = json.loads(manifest.read_text(encoding='utf-8'))
    if data['archive'] != 'experiment-results.zip':
        raise ValueError('unexpected archive filename')
    archive = manifest.parent/data['archive']
    sums = dict(line.split('  ', 1)[::-1] for line in (manifest.parent/'SHA256SUMS').read_text().splitlines())
    actual = sha(archive)
    if sums.get(manifest.name) != sha(manifest) or sums.get(archive.name) != actual or actual != data['archive_sha256']:
        raise ValueError('archive/manifest checksum mismatch')
    if archive.stat().st_size != data['archive_bytes']:
        raise ValueError('archive size mismatch')
    expected = {e['path']: e for e in data['files']}
    if not expected or len(expected) != len(data['files']) or len(expected) != data['file_count']:
        raise ValueError('empty/duplicate inventory')
    with zipfile.ZipFile(archive) as zipped:
        if len(zipped.namelist()) != len(expected) or set(zipped.namelist()) != set(expected):
            raise ValueError('archive members differ')
        for name, entry in expected.items():
            safe_path(name)
            with zipped.open(name) as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            if digest != entry['sha256'] or zipped.getinfo(name).file_size != entry['bytes']:
                raise ValueError(f'corrupt member: {name}')
    return data


def restore(manifest, destination):
    data = verify(manifest)
    destination = Path(destination).resolve()
    # Require a new directory: no existing files can ever be overwritten.
    destination.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(Path(manifest).resolve().parent/data['archive']) as zipped:
        for entry in data['files']:
            target = (destination/safe_path(entry['path'])).resolve()
            if destination not in target.parents:
                raise ValueError('target escapes destination')
            target.parent.mkdir(parents=True, exist_ok=True)
            with zipped.open(entry['path']) as src, target.open('xb') as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            if sha(target) != entry['sha256']:
                raise ValueError(f'restored SHA mismatch: {target}')
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('create', 'verify', 'restore'))
    parser.add_argument('--repo', type=Path, default=Path('.'))
    parser.add_argument('--archive-dir', type=Path)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--destination', type=Path)
    args = parser.parse_args()
    if args.command == 'create':
        if not args.archive_dir:
            parser.error('--archive-dir required')
        directory, repo = args.archive_dir.resolve(), args.repo.resolve()
        if directory == repo or repo in directory.parents:
            parser.error('archive must be outside repository')
        commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
        print(create(directory, inventory(repo), commit), flush=True)
    else:
        if not args.manifest or (args.command == 'restore' and not args.destination):
            parser.error('--manifest required; restore also requires --destination')
        data = restore(args.manifest, args.destination) if args.command == 'restore' else verify(args.manifest)
        print(f"PASS: {data['file_count']} files, {data['uncompressed_bytes']} bytes; every member SHA verified", flush=True)


if __name__ == '__main__':
    main()
