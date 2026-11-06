"""Build minimal pinned integration snapshots in NEW detached worktrees.

No remote writes, merges, performance runs, or edits to the source checkout.
Run: python prepare.py --repo /path/to/repo --output /new/empty/directory
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

BASE = 'c9a81e637c18c628c708c59a1816708bce6e0847'
SQL = '9da066efb9c42947f683afd14dc26620c43156b1'
KAFKA = '64445776dd240b7e441f0babbdd6a68df6ecbc28'
V8_FILES = [
    'db/migrations/V8__diagnostic_join_fk_lock.sql',
    'db/p01_integration_test.go', 'db/upgrade_v5_integration_test.go',
    'db/upgrade_v7_integration_test.go', 'db/upgrade_v8_integration_test.go',
    'internal/modules/telemetry/archive/postgres/archive_test.go',
    'internal/modules/telemetry/archive/postgres/inventory_test.go',
    'internal/modules/telemetry/archive/postgres/real_d2_test.go',
    'internal/modules/detection/repository/postgres/join_lock_integration_test.go',
    'internal/modules/events/repository/postgres/join_atomicity_integration_test.go',
]
JOIN = 'internal/modules/events/repository/postgres/join.go'
BUDGET = 'internal/modules/events/repository/postgres/join_payload_budget_integration_test.go'


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args])


def copy(repo, target, ref, paths):
    for name in paths:
        p = target / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(git(repo, 'show', f'{ref}:{name}'))


def d1(repo, target):
    # These exact removals exclude P14 profiling only. All D1 logic is retained.
    code = git(repo, 'show', f'{SQL}:{JOIN}').decode()
    for line in [
        '\t"github.com/butorovv/cluster-anomaly-detection-v2/internal/platform/observability"\n',
        '\tdefer observability.Stage(s.Stages, "join_total")()\n',
        '\tdecodeDone := observability.Stage(s.Stages, "join_fetch_and_json_decode")\n',
        '\tdecodeDone()\n',
        '\tannotateDone := observability.Stage(s.Stages, "join_annotate_outside")\n',
        '\tannotateDone()\n',
        '\tfinalDone := observability.Stage(s.Stages, "join_final_envelopes_and_sql")\n',
        '\tfinalDone()\n',
    ]:
        if code.count(line) != 1:
            raise RuntimeError(f'Unexpected SQL source while removing profiling: {line!r}')
        code = code.replace(line, '')
    (target / JOIN).write_text(code)
    copy(repo, target, SQL, [BUDGET])


def commit(target, message):
    git(target, 'add', '--all')
    git(target, 'commit', '-m', message)
    return git(target, 'rev-parse', 'HEAD').decode().strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    repo, out = a.repo.resolve(), a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    for ref in (BASE, SQL, KAFKA):
        git(repo, 'cat-file', '-e', ref + '^{commit}')
    manifest = {'sources': {'P13': BASE, 'SQL': SQL, 'Kafka': KAFKA}, 'variants': {}}
    kafka_files = git(repo, 'diff', '--name-only', BASE, KAFKA).decode().splitlines()
    for label, v8, readiness, kafka in [
        ('A', False, False, False), ('B', True, True, False),
        ('C', False, False, True), ('D', True, True, True),
        ('D1-only', False, True, False), ('V8-only', True, False, False),
    ]:
        target = out / label
        git(repo, 'worktree', 'add', '--detach', str(target), BASE)
        if v8:
            copy(repo, target, SQL, V8_FILES)
        if readiness:
            d1(repo, target)
        if kafka:
            copy(repo, target, KAFKA, kafka_files)
        sha = commit(target, f'test(p14-integration): isolate {label} from P13') if label != 'A' else BASE
        names = git(target, 'ls-files').decode().splitlines()
        files = {name: {'sha256': hashlib.sha256((target / name).read_bytes()).hexdigest(),
                        'blob': git(target, 'rev-parse', f'HEAD:{name}').decode().strip()}
                 for name in names}
        manifest['variants'][label] = {'commit': sha, 'tree': git(target, 'rev-parse', 'HEAD^{tree}').decode().strip(),
                                      'path': str(target), 'D1': readiness, 'V8': v8, 'Kafka': kafka,
                                      'files': files}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({k: {'commit': v['commit'], 'path': v['path']} for k, v in manifest['variants'].items()}, indent=2))


if __name__ == '__main__':
    main()
