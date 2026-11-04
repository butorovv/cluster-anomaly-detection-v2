"""Lossless local backup of tracked raw resource samples; never removes inputs."""
import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path, PurePosixPath


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def member_path(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or '\\' in name or ':' in name:
        raise ValueError(f'unsafe archive member: {name}')
    if not name.startswith('experiments/artifacts/') or path.name != 'metrics-samples.json':
        raise ValueError(f'unexpected raw sample member: {name}')
    return path


def verify_members(archive, entries):
    expected = {entry['path']: entry for entry in entries}
    if len(expected) != len(entries) or not entries:
        raise ValueError('empty or duplicate sample inventory')
    with zipfile.ZipFile(archive) as zipped:
        names = zipped.namelist()
        if len(names) != len(expected) or set(names) != set(expected):
            raise ValueError('archive members differ from manifest')
        for name in names:
            member_path(name)
            with zipped.open(name) as stream:
                actual = hashlib.file_digest(stream, 'sha256').hexdigest()
            if actual != expected[name]['sha256'] or zipped.getinfo(name).file_size != expected[name]['bytes']:
                raise ValueError(f'corrupt archived sample: {name}')


def create_archive(repo, directory, paths, source_commit):
    repo, directory = Path(repo).resolve(), Path(directory).resolve()
    if directory == repo or repo in directory.parents:
        raise ValueError('archive directory must be outside the repository')
    entries = []
    for name in sorted(paths):
        member_path(name)
        path = (repo / name).resolve()
        if repo not in path.parents:
            raise ValueError('sample escapes repository')
        data = path.read_bytes()
        entries.append({'path': name, 'bytes': len(data), 'lines': len(data.splitlines()),
                        'sha256': hashlib.sha256(data).hexdigest()})
    if not entries:
        raise ValueError('no raw samples found; use verify for an existing backup')
    directory.mkdir(parents=True, exist_ok=False)
    archive = directory / 'metrics-samples.zip'
    with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipped:
        for entry in entries:
            zipped.write(repo / entry['path'], arcname=entry['path'])
    # Read and hash EVERY decompressed member before declaring a usable backup.
    verify_members(archive, entries)
    manifest = {'schema_version': 1, 'source_commit': source_commit, 'original_repo': str(repo),
                'archive': {'file': archive.name, 'bytes': archive.stat().st_size, 'sha256': sha(archive)},
                'files': entries, 'verified_uncompressed_members': len(entries),
                'verification': 'PASS: exact size/SHA-256 of every decompressed member; source files untouched'}
    path = directory / 'manifest.json'
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(manifest, stream, indent=2)
        stream.write('\n')
    with (directory / 'SHA256SUMS').open('x', encoding='ascii', newline='\n') as stream:
        stream.write(f"{sha(archive)}  {archive.name}\n{sha(path)}  {path.name}\n")
    verify_archive(path, source=repo)
    return path


def verify_archive(path, source=None):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text(encoding='utf-8'))
    if manifest['archive']['file'] != 'metrics-samples.zip':
        raise ValueError('unexpected archive filename')
    archive = path.parent / manifest['archive']['file']
    sums = dict(line.split('  ', 1)[::-1] for line in (path.parent/'SHA256SUMS').read_text().splitlines())
    if sums.get(path.name) != sha(path) or sums.get(archive.name) != sha(archive):
        raise ValueError('archive/manifest SHA256SUMS mismatch')
    if sha(archive) != manifest['archive']['sha256'] or archive.stat().st_size != manifest['archive']['bytes']:
        raise ValueError('archive bytes differ from manifest')
    verify_members(archive, manifest['files'])
    if source is not None:
        for entry in manifest['files']:
            if sha(Path(source)/entry['path']) != entry['sha256']:
                raise ValueError(f"working sample differs from backup: {entry['path']}")
    return manifest


def restore_archive(manifest_path, destination):
    manifest = verify_archive(manifest_path)
    destination = Path(destination).resolve()
    plan = []
    # Check every existing target BEFORE writing anything; never overwrite data.
    for entry in manifest['files']:
        relative = member_path(entry['path'])
        target = (destination / relative).resolve()
        if destination not in target.parents:
            raise ValueError('restore target escapes destination')
        if target.exists():
            if sha(target) != entry['sha256']:
                raise ValueError(f'different file already exists: {target}')
        else:
            plan.append((entry, target))
    with zipfile.ZipFile(Path(manifest_path).resolve().parent/manifest['archive']['file']) as zipped:
        for entry, target in plan:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open('xb') as output, zipped.open(entry['path']) as stream:
                while chunk := stream.read(1 << 20):
                    output.write(chunk)
            if sha(target) != entry['sha256']:
                raise ValueError(f'restored bytes differ: {target}')
    return len(plan)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    create = sub.add_parser('create')
    create.add_argument('--repo', type=Path, default=Path('.'))
    create.add_argument('--archive-dir', type=Path, required=True)
    create.add_argument('--tracked-only', action='store_true', help='archive only samples still tracked by Git')
    verify = sub.add_parser('verify')
    verify.add_argument('--manifest', type=Path, required=True)
    verify.add_argument('--source', type=Path)
    restore = sub.add_parser('restore')
    restore.add_argument('--manifest', type=Path, required=True)
    restore.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'create':
        repo = args.repo.resolve()
        tracked = {p for p in subprocess.check_output(['git', '-C', str(repo), 'ls-files', 'experiments/artifacts'], text=True).splitlines()
                   if p.endswith('/metrics-samples.json')}
        paths = sorted(tracked) if args.tracked_only else sorted(p.relative_to(repo).as_posix() for p in (repo/'experiments/artifacts').rglob('metrics-samples.json'))
        for path in paths:
            if path not in tracked:
                continue
            committed = subprocess.check_output(['git', '-C', str(repo), 'show', 'HEAD:'+path])
            if hashlib.sha256(committed).hexdigest() != sha(repo/path):
                raise ValueError(f'working sample differs from committed source: {path}')
        commit = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
        path = create_archive(repo, args.archive_dir, paths, commit)
        print(path)
    elif args.command == 'verify':
        manifest = verify_archive(args.manifest, args.source)
        print(f"PASS: {len(manifest['files'])} members, archive and manifest SHA-256 verified")
    else:
        print(f'PASS: restored {restore_archive(args.manifest, args.destination)} files; existing identical files retained')


if __name__ == '__main__':
    main()
