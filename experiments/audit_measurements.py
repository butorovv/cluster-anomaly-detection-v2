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
    received, commits, pending = [], [], []
    for message, item in intake.items():
        received_at = seconds(item['received_at'])
        sent = max(t for t in attempts[message] if t <= received_at)
        received.append(received_at)
        commits.append(seconds(item['committed_at']))
        pending.extend([(sent, 1), (received_at, -1)])
    depth, max_depth = 0, 0
    for _, delta in sorted(pending):
        depth += delta
        max_depth = max(max_depth, depth)
    master_sum = sum(seconds(i['committed_at'])-seconds(i['received_at']) for i in intake.values())
    intake_span = max(commits)-min(received)
    send_wait, source_durable, reconstruction_errors, join_wait = [], [], [], []
    joins = {}
    for item in intake.values():
        if item['kind'] == 'tick_fragment':
            joins[item['tick']] = max(joins.get(item['tick'], 0), seconds(item['committed_at']))
    for key, tick in ticks.items():
        identity = ['cad-output-v1', run['run_id'], tick['group'], 'tick_fragment', tick['tick'], 1]
        message = hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
        item, ack = intake[message], durable[key]
        source, computed = seconds(tick['source_first_at']), seconds(tick['computed_at'])
        received_at, committed = seconds(item['received_at']), seconds(item['committed_at'])
        confirmed = seconds(ack['confirmed_at'])
        sent = max(t for t in attempts[message] if t <= received_at)
        # COMMIT confirmation can follow outbox visibility: retain signed overlap.
        send_wait.append(sent-confirmed)
        source_durable.append(confirmed-source)
        join_wait.append(joins[tick['tick']]-committed)
        components = [(computed-source), (confirmed-computed), (sent-confirmed),
                      (received_at-sent), (committed-received_at), (joins[tick['tick']]-committed)]
        reconstruction_errors.append(abs(sum(components)-(joins[tick['tick']]-source)))
    return {'scenario': run['configuration']['name'], 'status': 'PASS',
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
            'input_kafka_lag_status': 'NOT_VERIFIED_LEGACY_SAMPLER',
            'broker_vs_consumer_local_queue': 'NOT_MEASURED: old traces have neither produce ACK nor PollRecords return timestamps',
            'postgres_lock_wait_seconds': 'NOT_MEASURED: intake service includes decode/validation/SQL/commit; no historical pg_stat_activity samples'}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--run', type=Path, action='append', required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    results = [analyze(path) for path in args.run]
    with args.output.open('x', encoding='utf-8') as f:
        json.dump({'schema_version': 1, 'runs': results,
                   'interpretation': 'sum_intake_service_over_span is serialized-loop occupancy only for Kafka; HTTP intervals overlap; quantiles of components are not additive'}, f, indent=2)
        f.write('\n')


if __name__ == '__main__':
    main()
