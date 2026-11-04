"""Recompute latency boundaries from real saved traces; never infer broker RTT."""
import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path


def seconds(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def distribution(values):
    values = sorted(values)
    if not values:
        raise ValueError('empty samples')
    if any(not math.isfinite(x) or x < -0.002 for x in values):
        raise ValueError('invalid clocks')
    return {'count': len(values), 'mean_seconds': sum(values)/len(values),
            **{q: values[math.ceil(p*len(values))-1] for q, p in
               [('p50_seconds', .5), ('p95_seconds', .95), ('p99_seconds', .99)]}}


def analyze(path):
    run = json.loads(path.read_text(encoding='utf-8-sig'))
    root = Path(run['log_directory'])
    ticks, durable, attempts, intake = {}, {}, {}, {}
    sources = []
    for log in sorted(root.glob('*.log')):
        sources.append({'path': str(log), 'sha256': hashlib.file_digest(log.open('rb'), 'sha256').hexdigest()})
        with log.open(encoding='utf-8-sig') as f:
            for line in f:
                try:
                    item = json.loads(line)
                except ValueError:
                    continue
                if item.get('execution') != run['execution_id']:
                    continue
                key = item.get('group', '') + '/' + item.get('tick', '')
                if item['msg'] == 'tick measurement':
                    ticks[key] = item
                elif item['msg'] == 'tick durable':
                    durable[key] = item
                elif item['msg'] == 'output attempt':
                    attempts.setdefault(item['message_id'], []).append(seconds(item['sent_at']))
                elif item['msg'] == 'result measurement':
                    prior = intake.get(item['message_id'])
                    if prior is None or seconds(item['committed_at']) < seconds(prior['committed_at']):
                        intake[item['message_id']] = item
    if len(ticks) != run['comparison']['raw_rows']:
        raise ValueError('missing tick traces')
    received, commits, pending, transport, service = [], [], [], [], []
    for message, item in intake.items():
        received_at = seconds(item['received_at'])
        sent = max(t for t in attempts[message] if t <= received_at)
        received.append(received_at)
        commits.append(seconds(item['committed_at']))
        pending.extend([(sent, 1), (received_at, -1)])
        transport.append(received_at-sent)
        service.append(seconds(item['committed_at'])-received_at)
    depth, max_depth = 0, 0
    for _, delta in sorted(pending):
        depth += delta
        max_depth = max(max_depth, depth)
    master_sum = sum(seconds(i['committed_at'])-seconds(i['received_at']) for i in intake.values())
    intake_span = max(commits)-min(received)
    send_wait, source_durable, reconstruction_errors, join_wait, e2e, source_times = [], [], [], [], [], []
    joins = {}
    for item in intake.values():
        if item['kind'] == 'tick_fragment':
            joins[item['tick']] = max(joins.get(item['tick'], 0), seconds(item['committed_at']))
    for key, tick in ticks.items():
        identity = ['cad-output-v1', run['run_id'], tick['group'], 'tick_fragment', tick['tick'], 1]
        message = hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
        item, ack = intake[message], durable[key]
        source, computed = seconds(tick['source_first_at']), seconds(tick['computed_at'])
        source_times.append(source)
        received_at, committed = seconds(item['received_at']), seconds(item['committed_at'])
        confirmed = seconds(ack['confirmed_at'])
        sent = max(t for t in attempts[message] if t <= received_at)
        # COMMIT confirmation can follow outbox visibility: retain signed overlap.
        send_wait.append(sent-confirmed)
        source_durable.append(confirmed-source)
        join_wait.append(joins[tick['tick']]-committed)
        e2e.append(joins[tick['tick']]-source)
        components = [(computed-source), (confirmed-computed), (sent-confirmed),
                      (received_at-sent), (committed-received_at), (joins[tick['tick']]-committed)]
        reconstruction_errors.append(abs(sum(components)-(joins[tick['tick']]-source)))
    verified = {}
    for name, values in [('send_to_master_receive', transport), ('master_intake_to_commit_confirmation', service),
                         ('source_to_final_join_confirmation', e2e), ('compute', [t['compute_seconds'] for t in ticks.values()]),
                         ('computed_to_durable_confirmation', [t['durability_wait_seconds'] for t in durable.values()])]:
        actual = distribution(values)
        saved = run['measurements'][name]
        if actual['count'] != saved['count'] or any(abs(actual[q]-saved[q]) > 0.00001 for q in
                                                  ['mean_seconds', 'p50_seconds', 'p95_seconds', 'p99_seconds']):
            raise ValueError(f'saved quantiles differ from actual traces: {name}')
        verified[name] = actual
    throughput = len(ticks)/(max(joins.values())-min(source_times))
    if abs(throughput-run['measurements']['group_ticks_per_second']) > .00001:
        raise ValueError('saved throughput differs from real processing clocks')
    samples = json.loads((root/'metrics-samples.json').read_text(encoding='utf-8-sig'))
    resources = {}
    for source, target in [('WorkerCPU', 'worker_cpu_seconds'), ('MasterCPU', 'master_cpu_seconds'),
                           ('WorkerRSS', 'worker_peak_aggregate_rss_bytes'), ('MasterRSS', 'master_peak_rss_bytes'),
                           ('Backlog', 'outbox_backlog_max'), ('Lag', 'input_kafka_lag_records_max')]:
        value = max(s.get(source, 0) for s in samples)
        if abs(value-run['measurements'][target]) > .00001:
            raise ValueError(f'saved resource aggregate differs from samples: {target}')
        resources[target] = value
    for key, value in run['measurements']['counters'].items():
        if max(s.get('counters', {}).get(key, 0) for s in samples) != value:
            raise ValueError(f'saved counter differs from samples: {key}')
    return {'scenario': run['configuration']['name'], 'status': 'PASS',
            'verified_distributions': verified, 'verified_throughput': throughput,
            'verified_resource_aggregates': resources,
            'resource_samples_sha256': hashlib.sha256((root/'metrics-samples.json').read_bytes()).hexdigest(),
            'source_result_sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'log_sources': sources,
            'source_to_durable': distribution(source_durable),
            'durable_ack_to_send_signed_seconds': {'min': min(send_wait), 'mean': sum(send_wait)/len(send_wait),
                                                  'p95': sorted(send_wait)[math.ceil(.95*len(send_wait))-1], 'overlap_count': sum(x < 0 for x in send_wait)},
            'own_commit_to_final_join': distribution(join_wait),
            'max_e2e_telescope_error_seconds': max(reconstruction_errors),
            'master_intake_span_seconds': intake_span, 'sum_master_intake_to_commit_seconds': master_sum,
            'sum_intake_service_over_span': master_sum/intake_span,
            'max_sent_not_yet_intaken_messages': max_depth,
            'worker_cpu_seconds': run['measurements']['worker_cpu_seconds'],
            'master_cpu_seconds': run['measurements']['master_cpu_seconds'],
            'input_kafka_lag_status': run['measurements'].get('input_kafka_lag_status', 'NOT_VERIFIED_LEGACY_SAMPLER'),
            'broker_vs_consumer_local_queue': 'NOT_MEASURED: old traces have neither produce ACK nor PollRecords return timestamps',
            'postgres_lock_wait_seconds': 'NOT_MEASURED: intake service includes decode/validation/SQL/commit; no historical pg_stat_activity samples'}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', type=Path, action='append', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    results = [analyze(path) for path in args.run]
    with args.output.open('x', encoding='utf-8', newline='\n') as f:
        json.dump({'schema_version': 1, 'runs': results,
                   'interpretation': 'sum_intake_service_over_span is serialized-loop occupancy only for Kafka; HTTP intervals overlap; quantiles of components are not additive'}, f, indent=2)
        f.write('\n')


if __name__ == '__main__':
    main()
