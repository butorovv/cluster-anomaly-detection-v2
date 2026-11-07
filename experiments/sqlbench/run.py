"""P12 Compose factorial benchmark. Default is a plan; --run requires Linux gates.

No automatic Docker/WSL startup, global changes, pruning or volume deletion.
Pilot (60 ticks, 4 Workers) precedes every full series. Three matched randomized
blocks = twelve full attempts, no replacement of failures. SQL-observed runs are
separate diagnostics and never pooled with clean throughput.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import secrets
import shutil
import subprocess
import time
import uuid

VARIANTS = ('p13','d1','v8','d1-v8')
PIN = 'c6de846b05330a0eb3c1f67dae19dc14a4a2e9cc'
PROFILE = 'a39a7f63ad7cea88ac0a642ecd1a38c11ef3ea727fe97d654bb4cb7297f77a36'
INPUT = 'cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7'
GIB = 1 << 30
TARGET = ['go','test','-count=1','-p=1','./db','./internal/modules/events/repository/postgres','./internal/modules/detection/repository/postgres',
          '-run','TestOriginalP01SQLSuites|TestP01ConcurrentFencingAndCrash|TestDurableJoinDuplicateReorderRestart|TestConcurrentTimestampJoinLockOrder|TestDiagnosticCommitOutcomeReconcilesAllOutputs|TestStoreStaleEpochRace']


def sha(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f,'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')


def resource_reasons(memory, disk, cpus, load):
    reasons=[]
    if memory < 12*GIB: reasons.append('MemAvailable below 12 GiB (8 GiB container limits + 4 GiB reserve)')
    if disk < 80*GIB: reasons.append('free disk below 80 GiB (retained Kafka data, outputs and builds)')
    if cpus < 12: reasons.append('fewer than 12 logical CPUs for fixed 10 CPU total quotas + reserve')
    if load > cpus/4: reasons.append('one-minute load exceeds 25% of logical CPU count')
    return reasons


def preflight(output):
    data={'platform':platform.platform(),'at':time.time(),'reasons':[]}
    if platform.system()!='Linux':
        data['reasons'].append('runner requires dedicated Linux host; no WSL auto-start')
    else:
        mem={s.split(':')[0]:int(s.split()[1])*1024 for s in Path('/proc/meminfo').read_text().splitlines() if ':' in s and s.split()[1].isdigit()}
        data.update(memory_available=mem['MemAvailable'],disk_free=shutil.disk_usage(output).free,cpus=os.cpu_count() or 1,load1=os.getloadavg()[0])
        data['reasons']+=resource_reasons(data['memory_available'],data['disk_free'],data['cpus'],data['load1'])
        if mem.get('SwapFree',0)<mem.get('SwapTotal',0): data['reasons'].append('swap already in use; postpone stable benchmark')
    try:
        info=json.loads(subprocess.check_output(['docker','info','--format','{{json .}}'],stderr=subprocess.DEVNULL))
        data['docker']={k:info.get(k) for k in ('ServerVersion','NCPU','MemTotal','OSType','MemoryLimit','SwapLimit','DockerRootDir')}
        if info.get('OSType')!='linux' or not info.get('MemoryLimit') or not info.get('SwapLimit'):
            data['reasons'].append('Linux cgroup memory/swap constraints not confirmed')
        if info.get('MemTotal',0)<12*GIB or info.get('NCPU',0)<12:
            data['reasons'].append('Docker engine resources below declared host budget')
        docker_disk=shutil.disk_usage(info['DockerRootDir']).free
        data['docker_disk_free']=docker_disk
        if docker_disk < 80*GIB: data['reasons'].append('Docker data filesystem has less than 80 GiB free')
    except (OSError,subprocess.SubprocessError,ValueError,KeyError):
        data['reasons'].append('Docker engine/resources unavailable')
    data['status']='READY' if not data['reasons'] else 'BLOCKED_RESOURCES'
    return data


def schedule(seed=141):
    rng=random.Random(seed)
    blocks=[]
    for repeat in range(1,4):
        order=list(VARIANTS)
        rng.shuffle(order)
        blocks += [{'kind':'full','variant':name,'repeat':repeat} for name in order]
    return [{'kind':'pilot','variant':n,'repeat':0} for n in VARIANTS]+blocks


def verify_inputs(assets,golden,manifest):
    profile=assets/'configs/group-profile.summit-v1.json'
    if sha(profile)!=PROFILE: raise ValueError('profile SHA mismatch')
    p=json.loads(profile.read_text())
    for name,want in p['input_sha256'].items():
        if sha(assets/name)!=want: raise ValueError('asset checksum differs: '+name)
    entries=json.loads(manifest.read_text())
    selected=[x for x in entries if x['groups']==16 and x['delta']==0]
    if len(selected)!=1 or selected[0]['source_commit']!=PIN: raise ValueError('independent golden pin missing')
    seen=set()
    for item in selected[0]['files']:
        # Original manifest uses Windows paths. Only basenames are relocated.
        name=item['path'].replace('\\','/').split('/')[-1]
        if name in seen or sha(golden/'g16-d0'/name)!=item['sha256']: raise ValueError('golden checksum differs: '+name)
        seen.add(name)
    if seen != {'raw.ndjson','temporal.ndjson','evidence.ndjson','events.ndjson','summary.json'}: raise ValueError('unexpected golden set')
    return {'profile':PROFILE,'input':INPUT,'golden_manifest_sha256':sha(manifest),'v1_pin':PIN}


def relocate_manifest(entries):
    # Go filepath.Base on Linux cannot parse historical Windows backslashes.
    # Only locations change; source pin, per-file SHA and every other field stay.
    entries=json.loads(json.dumps(entries))
    for entry in entries:
        for item in entry['files']:
            name=item['path'].replace('\\','/').split('/')[-1]
            item['path']=f"/golden/g{entry['groups']}-d{entry['delta']}/{name}"
    return entries


def inspect_redacted(ids, env):
    info=json.loads(subprocess.check_output(['docker','inspect',*ids],env=env))
    for item in info:
        item['Config']['Env']=[x.split('=',1)[0]+'=<redacted>' for x in item['Config'].get('Env',[])]
    return info


def run(a):
    a.output.mkdir(parents=True,exist_ok=False)
    variants=json.loads((a.sources/'variants.json').read_text())
    if [x['name'] for x in variants['variants']]!=list(VARIANTS): raise ValueError('four exact variants required')
    for v in variants['variants']:
        if sha(a.sources/v['archive'])!=v['archive_sha256']: raise ValueError('source archive mismatch')
        root=a.sources/v['name']
        actual={p.relative_to(root).as_posix():sha(p) for p in root.rglob('*') if p.is_file()}
        if actual!=v['files']: raise ValueError('materialized source changed: '+v['name'])
    jobs=schedule()
    write(a.output/'plan.json',{'jobs':jobs,'optional_diagnostic_jobs':[{'kind':'diagnostic','variant':n,'repeat':0} for n in VARIANTS],
       'full_budget':12,'pilot_budget':4,'optional_diagnostic_budget':4 if a.diagnostics else 0,'replacement_attempts':0,
       'limits':{'postgres':{'cpu':2,'memory_gib':2},'kafka':{'cpu':2,'memory_gib':2},'harness_and_children':{'cpu':6,'memory_gib':4}},
       'failure_policy':'retain every result; stop safety/unhealthy stand; never retry/replace a failed full sample; do not pool diagnostics/OS/stand contexts',
       'variant_source_commits':{v['name']:v['source_commit'] for v in variants['variants']}})
    state=preflight(a.output)
    write(a.output/'preflight.json',state)
    if not a.run:
        print('PLAN ONLY; '+state['status'])
        return
    if state['status']!='READY':
        write(a.output/'decision.json',{'status':'NOT_RUN_RESOURCE_GATE','reasons':state['reasons'],'completed_full':0})
        print('BLOCKED: '+ '; '.join(state['reasons']))
        return
    verified=verify_inputs(a.assets,a.golden,a.manifest)
    shutil.copyfile(a.manifest,a.output/'golden-manifest.original.json')
    write(a.output/'golden-manifest.json',relocate_manifest(json.loads(a.manifest.read_text())))
    verified['relocated_manifest_sha256']=sha(a.output/'golden-manifest.json')
    verified['manifest_relocation']='paths only; original retained; scientific pin/file SHA unchanged'
    write(a.output/'inputs.json',verified)
    # All credentials are new scoped test credentials; never export them in reports.
    env=dict(os.environ,EXECUTION_ID=str(uuid.uuid4()),ASSETS_ROOT=str(a.assets),OUTPUT_TRANSPORT='kafka',CHECKPOINT_INTERVAL_TICKS='3',GROUPS_COUNT='16',DELTA_SECONDS='0',REPLAY_SPEED='0',MEASURE='true',
             POSTGRES_PASSWORD=secrets.token_hex(24),WORKER_DB_PASSWORD=secrets.token_hex(24),MASTER_DB_PASSWORD=secrets.token_hex(24),
             SQLBENCH_RESULTS=str(a.output),SQLBENCH_GOLDEN=str(a.golden),PG_PORT='57415',KAFKA_PORT='30415',MASTER_PORT='58080')
    compose=['docker','compose','-p',a.project,'-f',str(a.repo/'deploy/compose.yaml'),'-f',str(a.repo/'experiments/sqlbench/compose.override.yaml')]
    records=[]
    current=None
    def execute(args,log,timeout=300):
        with (a.output/log).open('wb') as f:
            try: return subprocess.run(args,env=env,stdout=f,stderr=subprocess.STDOUT,timeout=timeout).returncode
            except subprocess.TimeoutExpired: return 124
    def c(*args,log,timeout=300): return execute(compose+list(args),log,timeout)
    def record(row):
        records.append(row)
        write(a.output/'all-attempts.json',records)
    # Do not attach to any existing project or reuse its state/volumes.
    existing=subprocess.check_output(['docker','ps','-aq','--filter','label=com.docker.compose.project='+a.project],env=env).decode().strip()
    volumes=subprocess.check_output(['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+a.project],env=env).decode().strip()
    if existing or volumes: raise ValueError('new isolated Compose project required; existing state preserved')
    if not a.project.startswith('cad-p14-1b-') or not all(x.isalnum() or x=='-' for x in a.project): raise ValueError('use fresh cad-p14-1b-* project')
    env.update(CAD_IMAGE='unused:plan',SQLBENCH_SOURCE_COMMIT=variants['variants'][0]['source_commit'])
    try:
        if c('up','-d','--wait','postgres','kafka',log='infrastructure.log'):
            record({'kind':'setup','status':'FAIL','reason':'P12 services not healthy'})
            return
        service_ids=subprocess.check_output(compose+['ps','-q','postgres','kafka'],env=env).decode().split()
        # Verify actual resource limits and image IDs before workload starts.
        info=inspect_redacted(service_ids,env)
        for i in info:
            h=i['HostConfig']
            if h['Memory']!=2*GIB or h['MemorySwap']!=2*GIB or h['NanoCpus']!=2_000_000_000: raise ValueError('effective Compose limits differ')
        write(a.output/'infrastructure-inspect.json',info)
        for v in variants['variants']:
            env.update(CAD_IMAGE='cad-p14-1b:'+v['source_commit'],SQLBENCH_SOURCE_COMMIT=v['source_commit'],CAD_SQL_TIMINGS='0')
            image=env['CAD_IMAGE']
            if execute(['docker','build','--target','build','-f',str(a.sources/v['name']/'deploy/Dockerfile'),'-t',image,str(a.sources/v['name'])],v['name']+'-build.log',1200):
                record({'kind':'build','variant':v['name'],'status':'FAIL'})
                return
            write(a.output/(v['name']+'-image.json'),json.loads(subprocess.check_output(['docker','image','inspect',image],env=env)))
            rc=c('run','--no-deps','--rm','-T','--entrypoint','go','bootstrap',*TARGET[1:],log=v['name']+'-targeted.log',timeout=300)
            record({'kind':'targeted','variant':v['name'],'source_commit':v['source_commit'],'status':'PASS' if rc==0 else 'FAIL','exit_code':rc})
            if rc: return
        # All pilots before ANY full attempt. Independent full golden is mandatory
        # for each full PASS; pilots never count as independent full golden.
        if a.diagnostics: jobs += [{'kind':'diagnostic','variant':n,'repeat':0} for n in VARIANTS]
        for job in jobs:
            guard=preflight(a.output)
            write(a.output/('preflight-'+str(len(records))+'.json'),guard)
            if guard['status']!='READY':
                record(dict(job,status='NOT_RUN_RESOURCE_GATE',reasons=guard['reasons']))
                break
            v=next(x for x in variants['variants'] if x['name']==job['variant'])
            name=f"{job['variant']}-{job['kind']}-{job['repeat']}"
            diagnostic=job['kind']=='diagnostic'
            env.update(CAD_IMAGE='cad-p14-1b:'+v['source_commit'],SQLBENCH_SOURCE_COMMIT=v['source_commit'],CAD_SQL_TIMINGS='1' if diagnostic else '0')
            spec=dict(name=name,workers=4,groups=16,delta=0,mode='kafka',cadence=3,poll_ms=100,replay_speed=0)
            if job['kind']=='pilot': spec['pilot_ticks']=60
            config=dict(schema_version=1,assets='/assets',profile_sha256=PROFILE,input_sha256=INPUT,brokers='kafka:9092',golden_root='/golden',golden_manifest='/bench/golden-manifest.json',output='/bench',timeout_seconds=1200,measure=True,profile=False,runs=[spec])
            write(a.output/(name+'-config.json'),config)
            current=dict(job,status='RUNNING_UNCONFIRMED',source_commit=v['source_commit'],started_at=time.time())
            record(current)  # durable intent even if Docker/observer/host fails
            container=a.project+'-'+name
            # Keep exited containers for resource/OOM evidence; no automatic rm.
            rc=c('run','--no-deps','-T','--name',container,'--entrypoint','go','bootstrap','run','./experiments/runner','--config','/bench/'+name+'-config.json',log=name+'-runner.log',timeout=1500)
            runtime=inspect_redacted([container],env)[0]
            write(a.output/(name+'-container.json'),runtime)
            result_path=a.output/(name+'.json')
            result=json.loads(result_path.read_text()) if result_path.exists() else None
            ok=rc==0 and result is not None and result.get('status')=='PASS'
            h=runtime['HostConfig']
            ok=ok and h['Memory']==4*GIB and h['MemorySwap']==4*GIB and h['NanoCpus']==6_000_000_000 and not runtime['State'].get('OOMKilled',False)
            if ok and job['kind']!='pilot': ok=result['comparison']['status']=='PASS' and result['comparison']['raw_rows']==46080
            if result is not None:
                ledger=result['environment'].get('schema_migrations',[])
                ok=ok and [x['version'] for x in ledger]==v['migration_versions'] and result['environment'].get('source_commit')==v['source_commit']
                ok=ok and all(x['sha256']==v['files'].get('db/migrations/'+x['name']) for x in ledger)
                ok=ok and result['environment'].get('postgres_fsync')=='on' and result['environment'].get('postgres_synchronous_commit')=='on'
            current.update(status='PASS' if ok else 'FAIL',exit_code=rc,golden='PILOT_NOT_FULL_GOLDEN' if job['kind']=='pilot' else (result or {}).get('comparison',{}).get('status','NOT_REACHED'),result=name+'.json' if result else None,included_in_clean_throughput=bool(ok and job['kind']=='full'),finished_at=time.time())
            write(a.output/'all-attempts.json',records)
            current=None
            if not ok: break  # failure retained; no replacement, stand needs review
            # Runtime CPU/RSS/backlog, active SQL waits and E2E are in Result and
            # raw samples. Separate diagnostics add SQL join/decode/COMMIT timing.
            time.sleep(30)
    finally:
        if current is not None and current['status']=='RUNNING_UNCONFIRMED':
            current.update(status='FAIL_INTERRUPTED',golden='NOT_REACHED',included_in_clean_throughput=False)
            write(a.output/'all-attempts.json',records)
        c('stop',log='stop.log',timeout=60)  # retain volumes, never down -v/prune
    write(a.output/'decision.json',{'status':'REVIEW_REQUIRED','completed_full':sum(r['kind']=='full' and r['status'] in ('PASS','FAIL') for r in records),'note':'No automated ACCEPT; inspect all failures, golden, dispersion and durability evidence; diagnostics never pooled'})


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo',type=Path,default=Path('.'))
    p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--assets',type=Path,required=True)
    p.add_argument('--golden',type=Path,required=True)
    p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--project',default='cad-p14-1b-'+uuid.uuid4().hex[:10])
    p.add_argument('--run',action='store_true')
    p.add_argument('--diagnostics',action='store_true',help='separate four SQL-observed full runs after clean series; never pooled')
    a=p.parse_args()
    for key in ('repo','sources','output','assets','golden','manifest'): setattr(a,key,getattr(a,key).resolve())
    run(a)
