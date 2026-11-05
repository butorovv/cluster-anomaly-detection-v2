"""Verify an archived source snapshot and derive (never overwrite) run provenance.

The legacy runner discovers HEAD from its build cwd. A source-only git archive
nested in another checkout inherits that checkout's HEAD. Verify every source
byte against a fresh git archive of the intended commit before correcting this
metadata in a new derived result. Original measurements/binary hashes stay intact.
"""
import argparse
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path, PurePosixPath


def sha(data):
    return hashlib.sha256(data).hexdigest()


def derive(run_path, snapshot_tar, snapshot_root, commit, relocate_from=None, relocate_to=None, resource_context=None):
    expected = subprocess.check_output(['git', 'archive', '--format=tar', commit])
    archived = snapshot_tar.read_bytes()
    if sha(expected) != sha(archived):
        raise ValueError('source archive is not the exact requested Git commit')
    members = set()
    with tarfile.open(snapshot_tar) as archive:
        for member in archive.getmembers():
            if not member.isfile():
                continue
            relative = Path(member.name)
            if relative.is_absolute() or '..' in relative.parts:
                raise ValueError('unsafe source snapshot member')
            members.add(relative.as_posix())
            disk = (snapshot_root/relative).read_bytes()
            if sha(disk) != sha(archive.extractfile(member).read()):
                raise ValueError(f'modified snapshot source: {member.name}')
    actual = {p.relative_to(snapshot_root).as_posix() for p in snapshot_root.rglob('*') if p.is_file()}
    if members != actual:
        raise ValueError('extra/missing files in source snapshot')
    original = run_path.read_bytes()
    run = json.loads(original.decode('utf-8-sig'))
    discovered = run['environment'].get('source_commit')
    run['environment']['source_commit'] = commit
    original_paths = {'log_directory': run.get('log_directory'), 'binary_paths': run.get('binary_paths')}
    if relocate_from is not None:
        def mapped(value):
            relative = PurePosixPath(value).relative_to(PurePosixPath(relocate_from))
            if '..' in relative.parts:
                raise ValueError('unsafe relocated artifact path')
            return str(Path(relocate_to).joinpath(*relative.parts))
        run['log_directory'] = mapped(run['log_directory'])
        run['binary_paths'] = {name: mapped(value) for name, value in run['binary_paths'].items()}
    if resource_context:
        run['environment']['resource_context_sha256'] = sha(resource_context.read_bytes())
    run['source_snapshot_provenance'] = {
        'original_result': str(run_path), 'original_result_sha256': sha(original),
        'legacy_build_cwd_head': discovered, 'verified_source_commit': commit,
        'source_archive': str(snapshot_tar), 'source_archive_sha256': sha(archived),
        'source_files_verified': len(members),
        'source_tree_sha': subprocess.check_output(['git', 'rev-parse', commit+'^{tree}'], text=True).strip(),
        'original_paths': original_paths,
        'path_relocation': {'from': relocate_from, 'to': str(relocate_to) if relocate_to else None},
        'note': 'Derived metadata correction; original result, measurements, binary hashes and comparison are preserved.'}
    return run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--snapshot-tar', type=Path, required=True)
    parser.add_argument('--snapshot-root', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--relocate-from')
    parser.add_argument('--relocate-to', type=Path)
    parser.add_argument('--resource-context', type=Path)
    args = parser.parse_args()
    if bool(args.relocate_from) != bool(args.relocate_to):
        parser.error('both relocation prefixes required')
    value = derive(args.run, args.snapshot_tar, args.snapshot_root, args.commit,
                   args.relocate_from, args.relocate_to, args.resource_context)
    with args.output.open('x', encoding='utf-8', newline='\n') as output:
        json.dump(value, output, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
