"""Verify every prepared file against the minimal source selection, not just HEAD."""
import argparse
import hashlib
import json
from pathlib import Path
from prepare import BASE, SQL, KAFKA, V8_FILES, JOIN, BUDGET, git


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--variants', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    m = json.loads((a.variants / 'manifest.json').read_text())
    repo = Path(m['variants']['A']['path'])
    kafka = set(git(repo, 'diff', '--name-only', BASE, KAFKA).decode().splitlines())
    basefiles = set(git(repo, 'ls-tree', '-r', '--name-only', BASE).decode().splitlines())
    checks = []
    for label, v in m['variants'].items():
        target = Path(v['path'])
        assert not git(target, 'status', '--porcelain').strip(), label + ' dirty'
        changed = set(git(target, 'diff', '--name-only', BASE, v['commit']).decode().splitlines())
        expected = (set(V8_FILES) if v['V8'] else set()) | ({JOIN, BUDGET} if v['D1'] else set()) | (kafka if v['Kafka'] else set())
        assert changed == expected, (label, changed ^ expected)
        assert set(v['files']) == basefiles | expected, label + ' unexpected files'
        for name, recorded in v['files'].items():
            actual = (target/name).read_bytes()
            assert hashlib.sha256(actual).hexdigest() == recorded['sha256'], (label, name, 'hash')
            assert actual == git(target, 'show', f"{v['commit']}:{name}"), (label, name, 'working bytes')
            if name == JOIN and v['D1']:
                # Source transform is explicitly documented by prepare.d1.
                source = git(repo, 'show', f'{SQL}:{JOIN}').decode()
                source = ''.join(line for line in source.splitlines(keepends=True)
                                 if not any(s in line for s in ('/platform/observability"', 'observability.Stage(', '\tdecodeDone()', '\tannotateDone()', '\tfinalDone()')))
                assert actual == source.encode(), (label, 'D1 transform')
            else:
                ref = KAFKA if v['Kafka'] and name in kafka else SQL if name in expected else BASE
                assert actual == git(repo, 'show', f'{ref}:{name}'), (label, name, 'source')
        migrations = {n: f['sha256'] for n, f in v['files'].items() if n.startswith('db/migrations/')}
        checks.append(dict(variant=label, commit=v['commit'], tree=v['tree'], changed_files=sorted(changed),
                           migrations=migrations, source_equality='PASS',
                           go_mod_unchanged=v['files']['go.mod']==m['variants']['A']['files']['go.mod'],
                           go_sum_unchanged=v['files']['go.sum']==m['variants']['A']['files']['go.sum']))
    assert m['variants']['C']['tree'] == git(repo, 'rev-parse', KAFKA+'^{tree}').decode().strip()
    a.output.write_text(json.dumps(checks, indent=2)+'\n')
    print('PASS: all six clean worktrees, complete file inventory, source transforms, migration hashes, dependencies and exact C tree')


if __name__ == '__main__':
    main()
