"""Materialize a pinned 2x2 SQL experiment; never edit a runtime checkout.

Synthetic source commits live under refs/p14-1b/<package>/<variant>. Their parent
is P13. The shared observation harness is identical in all four source trees.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile

BASE = 'c9a81e637c18c628c708c59a1816708bce6e0847'
HOOKS = '7220d7bbabe26c7b82586bc6d5a10f95d0972517'
D1 = 'e125d73af724b56516a228b9c31c535c432d516d'
V8 = 'b2af3fe8c44a2f3943c683d67360424922b8f7c3'
HARNESS = 'a23567b5855e0bbf54aa8ae463ab5e89f273419b'
JOIN = 'internal/modules/events/repository/postgres/join.go'
V8_PATH = 'db/migrations/V8__diagnostic_join_fk_lock.sql'
VARIANTS = ('p13', 'd1', 'v8', 'd1-v8')


def git(repo, *args, data=None, env=None):
    return subprocess.check_output(['git', '-C', str(repo), *args], input=data, env=env)


def blob(repo, ref, path):
    return git(repo, 'show', ref + ':' + path)


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('pinned harness patch anchor changed: ' + old[:90])
    return text.replace(old, new, 1)


def shared_runner(text):
    # A bounded short-window pilot is explicitly structural, never full golden.
    text = replace_once(text, 'type RunSpec struct {', 'type RunSpec struct {\n\tPilotTicks int `json:"pilot_ticks,omitempty"`')
    start = text.index('\tif commit, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output(); err == nil {')
    end = text.index('\n\tdefer func() { report.Finished', start)
    text = text[:start] + '\treport.Environment["source_commit"] = os.Getenv("SQLBENCH_SOURCE_COMMIT")\n\treport.Environment["sql_timing_observer"] = os.Getenv("CAD_SQL_TIMINGS") == "1"' + text[end:]
    text = replace_once(text, '\twindow := analysis.NIRWindows()[1]', '''
	window := analysis.NIRWindows()[1]
	if spec.PilotTicks != 0 {
		if spec.PilotTicks < 3 || spec.PilotTicks > 60 || spec.Fault != "" { return report, fmt.Errorf("pilot requires 3..60 ticks and no fault") }
		window.End = window.Start.Add(time.Duration(spec.PilotTicks)*telemetry.GridStep)
	}''')
    text = text.replace('joined < 2880', 'joined < int(window.End.Sub(window.Start)/telemetry.GridStep)')
    text = text.replace('results == 2880*spec.Groups', 'results == int(window.End.Sub(window.Start)/telemetry.GridStep)*spec.Groups')
    text = replace_once(text, '\t\tif cfg.Measure {\n\t\t\targs = append(args, "--measure=true")', '\t\targs = append(args, "--start", window.Start.Format(time.RFC3339), "--end", window.End.Format(time.RFC3339))\n\t\tif cfg.Measure {\n\t\t\targs = append(args, "--measure=true")')
    text = replace_once(text, '''	comparison, err := Compare(ctx, pool, execution, reference)
	report.Comparison = comparison
	if err != nil {
		return report, err
	}''', '''	var comparison Comparison
	if spec.PilotTicks == 0 {
		comparison, err = Compare(ctx, pool, execution, reference)
		if err != nil { report.Comparison = comparison; return report, err }
	} else {
		comparison = Comparison{Status:"PILOT_STRUCTURAL_ONLY", Raw:spec.PilotTicks*spec.Groups}
	}
	report.Comparison = comparison''')
    # Retain samples even on failure. No invented partial latency aggregates.
    text = replace_once(text, '''	if cfg.Measure {
		defer sampler.Stop()
	}''', '''	if cfg.Measure {
		defer func() {
			sampler.Stop()
			if e := saveJSON(filepath.Join(logDir,"metrics-samples.json"),sampler.Samples()); e != nil { retErr = errors.Join(retErr,e) }
			if e := saveJSON(filepath.Join(logDir,"client-timings.json"),sampler.last); e != nil { retErr = errors.Join(retErr,e) }
		}()
	}''')
    text = replace_once(text, '''	if cfg.Profile {
		if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");''', '''	if cfg.Profile || os.Getenv("CAD_SQL_TIMINGS") == "1" {
		if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");''')
    text = replace_once(text, 'report.Environment["profiling_enabled"] = true', 'report.Environment["profiling_enabled"] = cfg.Profile')
    # Record the actual ledger, not only how many migrations this call applied.
    text = replace_once(text, '\tvar serverVersion string', '''	var migrations []byte
	if err := pool.QueryRow(ctx,`SELECT jsonb_agg(jsonb_build_object('version',version,'name',name,'sha256',sha256) ORDER BY version) FROM public.schema_migrations`).Scan(&migrations); err != nil { return report,err }
	var applied any
	if err := json.Unmarshal(migrations,&applied); err != nil { return report,err }
	report.Environment["schema_migrations"] = applied
	var serverVersion string''')
    return text


def extract(repo, target):
    with tarfile.open(fileobj=io.BytesIO(git(repo, 'archive', BASE))) as archive:
        # Only trusted Git blobs, but still reject paths escaping the snapshot.
        for m in archive:
            path = target / m.name
            if m.name.startswith('/') or '..' in Path(m.name).parts or m.issym() or m.islnk():
                raise ValueError('unsafe source member')
            if m.isdir():
                path.mkdir(parents=True, exist_ok=True)
            elif m.isfile():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(archive.extractfile(m).read())
                path.chmod(m.mode)


def materialize(package, output):
    package,output=package.resolve(),output.resolve()
    data=json.loads((package/'variants.json').read_text(encoding='utf-8'))
    if [v['name'] for v in data['variants']]!=list(VARIANTS):
        raise ValueError('unexpected variant names')
    output.mkdir(parents=True,exist_ok=False)
    for v in data['variants']:
        archive=package/(v['name']+'.tar')
        if hashlib.sha256(archive.read_bytes()).hexdigest()!=v['archive_sha256']:
            raise ValueError('archive checksum mismatch')
        root=output/v['name']
        root.mkdir()
        with tarfile.open(archive) as source:
            for m in source:
                if m.name.startswith('/') or ':' in m.name or '\\' in m.name or '..' in Path(m.name).parts or m.issym() or m.islnk():
                    raise ValueError('unsafe source archive member')
                p=root/m.name
                if m.isdir(): p.mkdir(parents=True,exist_ok=True)
                elif m.isfile():
                    p.parent.mkdir(parents=True,exist_ok=True)
                    p.write_bytes(source.extractfile(m).read())
                    p.chmod(m.mode)
        files={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
        if files!=v['files']: raise ValueError('restored source mismatch')
        (output/archive.name).write_bytes(archive.read_bytes())
    (output/'variants.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    return data


def prepare(repo, output, package):
    repo, output = repo.resolve(), output.resolve()
    if not package.replace('-', '').isalnum():
        raise ValueError('alphanumeric/hyphen package name required')
    output.mkdir(parents=True, exist_ok=False)
    common = {}
    for p in ('internal/app/master/master.go', 'internal/app/worker/cli.go',
              'internal/platform/observability/metrics.go', 'internal/platform/observability/profile.go',
              'internal/platform/observability/profile_test.go', 'internal/modules/events/repository/postgres/master.go', JOIN):
        common[p] = blob(repo, HOOKS, p)
    paths = git(repo, 'ls-tree', '-r', '--name-only', HARNESS, 'internal/app/experiment', 'experiments/runner').decode().splitlines()
    for p in paths:
        common[p] = blob(repo, HARNESS, p)
    for p in ('internal/app/master/master.go', 'internal/app/worker/cli.go'):
        text = common[p].decode()
        common[p] = replace_once(text, 'if observability.ProfilingEnabled() {', 'if observability.ProfilingEnabled() || os.Getenv("CAD_SQL_TIMINGS") == "1" {').encode()
    common['internal/app/experiment/runner.go'] = shared_runner(common['internal/app/experiment/runner.go'].decode()).encode()
    d1_patch = git(repo, 'diff', D1+'^', D1, '--', JOIN)
    (output/'d1.patch').write_bytes(d1_patch)
    variants = []
    trees = {}
    for name in VARIANTS:
        root = output/name
        root.mkdir()
        extract(repo, root)
        for p, data in common.items():
            path = root/p
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        if name in ('d1', 'd1-v8'):
            subprocess.run(['git', 'apply', '--unsafe-paths', '--directory='+str(root), str(output/'d1.patch')], cwd=repo, check=True, capture_output=True)
        if name in ('v8', 'd1-v8'):
            (root/V8_PATH).write_bytes(blob(repo,V8,V8_PATH))
        gofiles = [str(root/p) for p in common if p.endswith('.go')]
        subprocess.run(['gofmt','-w',*gofiles],check=True)
        # Separate index; never checkout, stage or mutate the user's index/tree.
        index = output/(name+'.index')
        env = dict(os.environ, GIT_INDEX_FILE=str(index), GIT_WORK_TREE=str(root))
        git(repo,'read-tree',BASE,env=env)
        for p in sorted(set(common) | ({V8_PATH} if name in ('v8','d1-v8') else set())):
            oid = git(repo,'hash-object','-w','--stdin',data=(root/p).read_bytes()).decode().strip()
            git(repo,'update-index','--add','--cacheinfo','100644,'+oid+','+p,env=env)
        tree = git(repo,'write-tree',env=env).decode().strip()
        env.update(GIT_AUTHOR_NAME='P14 benchmark',GIT_AUTHOR_EMAIL='benchmark@local.invalid',GIT_COMMITTER_NAME='P14 benchmark',GIT_COMMITTER_EMAIL='benchmark@local.invalid',GIT_AUTHOR_DATE='2026-11-05T00:00:00Z',GIT_COMMITTER_DATE='2026-11-05T00:00:00Z')
        commit = git(repo,'commit-tree',tree,'-p',BASE,data=('P14-1B factorial source '+name+'; common observation harness\n').encode(),env=env).decode().strip()
        ref = 'refs/p14-1b/'+package+'/'+name
        git(repo,'update-ref',ref,commit,'0'*40)
        archive = output/(name+'.tar')
        archive.write_bytes(git(repo,'archive','--format=tar',commit))
        files = {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}
        # Reconcile ALL extracted bytes to the immutable source commit.
        with tarfile.open(archive) as tar:
            expected = {m.name:hashlib.sha256(tar.extractfile(m).read()).hexdigest() for m in tar if m.isfile()}
        if files != expected:
            raise ValueError('source tree contains uncommitted/missing bytes: '+name)
        trees[name]=files
        variants.append(dict(name=name,source_commit=commit,tree=tree,ref=ref,base=BASE,d1=name in ('d1','d1-v8'),v8=name in ('v8','d1-v8'),migration_versions=list(range(1,9 if name in ('v8','d1-v8') else 8)),archive=archive.name,archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),files=files))
    for left,right,allowed in [('p13','d1',{JOIN}),('v8','d1-v8',{JOIN}),('p13','v8',{V8_PATH}),('d1','d1-v8',{V8_PATH})]:
        changed={p for p in set(trees[left])|set(trees[right]) if trees[left].get(p)!=trees[right].get(p)}
        if changed != allowed:
            raise ValueError('unexpected factorial diff: '+repr(changed))
    bundle = output/'variants.bundle'
    git(repo,'bundle','create',str(bundle),*[v['ref'] for v in variants],'^'+BASE)
    git(repo,'bundle','verify',str(bundle))
    data=dict(schema_version=1,base=BASE,hook_source=HOOKS,harness_source=HARNESS,d1_source=D1,v8_source=V8,package=package,common_overlay_files=sorted(common),variant_contract='P13 runtime + identical observation/build harness; only factorial runtime differences are readiness join.go and immutable V8 SQL',variants=variants,bundle_sha256=hashlib.sha256(bundle.read_bytes()).hexdigest())
    (output/'variants.json').write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    return data


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,default=Path('.'))
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--package',default='p14-1b-20261105')
    parser.add_argument('--from-package',type=Path,help='restore verified exact snapshots without regenerating commits')
    a=parser.parse_args()
    result=materialize(a.from_package,a.output) if a.from_package else prepare(a.repo,a.output,a.package)
    for v in result['variants']: print(v['name'],v['source_commit'],v['archive_sha256'])
