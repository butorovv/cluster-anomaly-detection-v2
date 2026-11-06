"""Bounded correctness checks only; never runs a performance or full D2 series.

Requires prepared worktrees, Go 1.27.2, Docker images postgres:17/apache/kafka:4.1.2.
Creates only uniquely named containers without persistent volumes; cleanup finally.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time
import uuid


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--variants', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--stages', default='unit,sql,kafka,race', help='Resume completed preparation without repeating successful stages')
    a = p.parse_args()
    stages = set(a.stages.split(','))
    if not stages or stages - {'unit', 'sql', 'kafka', 'race'}:
        p.error('Unknown check stage')
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((a.variants / 'manifest.json').read_text())
    env = os.environ.copy()
    for k in list(env):
        if k.startswith(('SYSTEM_', 'P07_', 'ARCHIVE_REAL_', 'REAL_D2', 'TEST_DATABASE', 'DATABASE_URL')):
            env.pop(k)
    env.update(GOMAXPROCS='2', GOTOOLCHAIN='local', GOFLAGS='-mod=readonly')
    # Selectors cannot redirect Docker away from the managed local socket.
    for k in ('DOCKER_HOST', 'DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH'):
        env.pop(k, None)
    docker = ['docker', '--host=unix:///var/run/docker.sock']
    token = uuid.uuid4().hex[:10]
    pg, kafka = f'p14-check-pg-{token}', f'p14-check-kafka-{token}'
    owned, ledger, blocked = [], [], set()
    stop, unsafe = threading.Event(), threading.Event()
    latest = [time.monotonic()]

    def monitor(filename='resources.jsonl'):
        try:
            with (out / filename).open('x') as f:
                while not stop.is_set():
                    mem = {s.split(':')[0]: int(s.split()[1]) * 1024 for s in Path('/proc/meminfo').read_text().splitlines()}
                    current = int(Path('/sys/fs/cgroup/memory.current').read_text())
                    limit = int(Path('/sys/fs/cgroup/memory.max').read_text())
                    vm = dict(s.split() for s in Path('/proc/vmstat').read_text().splitlines())
                    sample = dict(time=time.time(), host_available=mem['MemAvailable'], cgroup_current=current,
                                  cgroup_available=limit-current, disk_free=shutil.disk_usage(out).free,
                                  pswpin=int(vm['pswpin']), pswpout=int(vm['pswpout']))
                    f.write(json.dumps(sample) + '\n'); f.flush()
                    latest[0] = time.monotonic()
                    if min(sample['host_available'], sample['cgroup_available']) < 600*1024**2 or sample['disk_free'] < 2*1024**3:
                        unsafe.set()
                    stop.wait(2)
        except Exception:
            unsafe.set()

    watch = threading.Thread(target=monitor, daemon=True)
    watch.start()

    def run(name, command, cwd=None, extra=None, timeout=1200):
        if unsafe.is_set() or time.monotonic() - latest[0] > 10:
            raise RuntimeError('Resource/freshness guard stopped checks')
        started = time.time()
        with (out / (name + '.log')).open('x') as log:
            proc = subprocess.Popen(command, cwd=cwd, env=env | (extra or {}), stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                while proc.poll() is None:
                    if unsafe.is_set() or time.monotonic()-latest[0] > 10 or time.time()-started > timeout:
                        ledger.append(dict(name=name, command=command, start=started, end=time.time(), stopped_by_guard=True))
                        raise RuntimeError('Resource/freshness/duration guard stopped ' + name)
                    time.sleep(0.25)
            except BaseException:
                if proc.poll() is None:
                    os.killpg(proc.pid, signal.SIGTERM)
                    try:
                        proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(proc.pid, signal.SIGKILL); proc.wait()
                raise
        ledger.append(dict(name=name, command=command, cwd=str(cwd), start=started, end=time.time(), exit=proc.returncode))
        (out / 'commands.json').write_text(json.dumps(ledger, indent=2) + '\n')
        print(name, 'PASS' if proc.returncode == 0 else 'FAIL', flush=True)
        return proc.returncode

    def wait_ready(container, command):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if subprocess.run(docker + ['exec', container] + command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10).returncode == 0:
                    return
            except subprocess.TimeoutExpired:
                pass
            if unsafe.is_set() or time.monotonic()-latest[0] > 10:
                raise RuntimeError('Resource guard during service readiness')
            time.sleep(1)
        raise RuntimeError('Service readiness timeout: ' + container)

    try:
        for label in (('A', 'B', 'C', 'D') if 'unit' in stages else ()):
            target = manifest['variants'][label]['path']
            if run(label+'-unit', ['go', 'test', '-json', '-p=1', '-count=1', '-timeout=10m', './...'], target):
                blocked.add(label)
            if run(label+'-vet', ['go', 'vet', '-p=1', './...'], target):
                blocked.add(label)
        # Dedicated correctness database; settings do not represent the laptop.
        if run('postgres-start', docker + ['run', '-d', '--name', pg, '--memory=1g', '--cpus=1', '--tmpfs', '/var/lib/postgresql/data:rw,size=768m', '-e', 'POSTGRES_PASSWORD=p14-disposable-only', '-p', '127.0.0.1:55432:5432', 'postgres:17', '-c', 'fsync=on', '-c', 'synchronous_commit=on', '-c', 'full_page_writes=on', '-c', 'log_lock_waits=on', '-c', 'deadlock_timeout=100ms']):
            raise RuntimeError('PostgreSQL start failed')
        owned.append(pg)
        wait_ready(pg, ['pg_isready', '-U', 'postgres'])
        dsn = 'postgres://postgres:p14-disposable-only@127.0.0.1:55432/postgres?sslmode=disable'
        sqlenv = {'TEST_DATABASE_URL': dsn, 'SYSTEM_TEST_DATABASE_URL': dsn, 'P07_TEST_DATABASE_URL': dsn}
        run('postgres-settings', docker + ['exec', pg, 'psql', '-U', 'postgres', '-Atc', "select name,setting,unit from pg_settings where name in ('server_version','shared_buffers','work_mem','max_connections','fsync','synchronous_commit','full_page_writes','default_transaction_isolation','wal_sync_method','lock_timeout','statement_timeout') order by name"])
        for label in (('A', 'B', 'C', 'D', 'D1-only', 'V8-only') if 'sql' in stages else ()):
            if label in blocked:
                continue
            target = manifest['variants'][label]['path']
            if run(label+'-sql', ['go', 'test', '-json', '-p=1', '-count=1', '-timeout=10m', './db', './internal/modules/detection/repository/postgres', './internal/modules/events/repository/postgres', './internal/modules/telemetry/archive/postgres'], target, sqlenv):
                blocked.add(label)
        if run('kafka-start', docker + ['run', '-d', '--name', kafka, '--memory=1536m', '--cpus=1', '-p', '127.0.0.1:59092:9092', '-e', 'KAFKA_HEAP_OPTS=-Xms256m -Xmx512m', '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller', '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:9094', '-e', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:59092,INTERNAL://localhost:9094', '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER', '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT', '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093', '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1', '-e', 'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1', '-e', 'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1', '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0', 'apache/kafka:4.1.2']):
            raise RuntimeError('Kafka start failed')
        owned.append(kafka)
        wait_ready(kafka, ['/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9094', '--list'])
        kenv = sqlenv | {'P07_TEST_KAFKA_BROKERS': '127.0.0.1:59092', 'P07_TEST_KAFKA_ISOLATED': '1', 'P07_WORKER_INTEGRATION': '1', 'SYSTEM_KAFKA_ISOLATED': '1', 'SYSTEM_KAFKA_INTEGRATION': '1'}
        for label in (('A', 'B', 'C', 'D') if 'kafka' in stages else ()):
            if label in blocked:
                continue
            target = manifest['variants'][label]['path']
            if run(label+'-kafka-worker', ['go', 'test', '-json', '-p=1', '-count=1', '-timeout=15m', './internal/modules/events/delivery/resulttransport', './internal/app/worker', './internal/modules/telemetry/delivery/kafkainput'], target, kenv):
                blocked.add(label)
        (out / 'status.json').write_text(json.dumps({'blocked_variants': sorted(blocked), 'performance': 'NOT_RUN', 'full_D2': 'NOT_RUN'}, indent=2))
    finally:
        for container in reversed(owned):
            for suffix, args in [('logs', ['logs', container]), ('inspect', ['inspect', '--format', '{{json .State}} {{json .Mounts}}', container])]:
                with (out / (container+'-'+suffix+'.log')).open('x') as f:
                    subprocess.run(docker+args, env=env, stdout=f, stderr=subprocess.STDOUT, timeout=20)
            subprocess.run(docker+['rm', '-f', '-v', container], env=env, stdout=subprocess.DEVNULL, timeout=30)
        stop.set(); watch.join(timeout=5)
        (out / 'commands.json').write_text(json.dumps(ledger, indent=2) + '\n')
        (out / 'cleanup.json').write_text(json.dumps({'owned_containers': owned, 'guard_triggered': unsafe.is_set(), 'remaining': subprocess.check_output(docker+['ps', '-a', '--format', '{{.Names}}'], env=env).decode().splitlines()}, indent=2))
    # Services are removed before race compilation to stay within cloud disk limits.
    # Live SQL/Kafka faults were checked above; race here covers in-process state.
    if 'race' in stages:
        stop.clear()
        latest[0] = time.monotonic()
        watch = threading.Thread(target=monitor, args=('resources-race.jsonl',), daemon=True)
        watch.start()
        try:
            for label in ('C', 'D'):
                if label in blocked:
                    continue
                target = manifest['variants'][label]['path']
                if run(label+'-race', ['go', 'test', '-json', '-race', '-p=1', '-count=1', '-timeout=10m', './internal/modules/events/usecase', './internal/modules/events/delivery/resulttransport'], target):
                    blocked.add(label)
        finally:
            stop.set(); watch.join(timeout=5)
            (out / 'commands.json').write_text(json.dumps(ledger, indent=2) + '\n')
            (out / 'status.json').write_text(json.dumps({'blocked_variants': sorted(blocked), 'performance': 'NOT_RUN', 'full_D2': 'NOT_RUN'}, indent=2))
    if blocked:
        raise SystemExit('Correctness failures: ' + ', '.join(sorted(blocked)))


if __name__ == '__main__':
    main()
