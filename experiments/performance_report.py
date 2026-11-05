"""Summarize independent runs without treating messages as independent trials.

Only PASS runs with independent golden parity enter performance comparisons.
Profile runs, fault runs and different commits/configurations remain separate.
All output is generated data and must be written outside tracked source files.
"""
import argparse
import hashlib
import json
import statistics
from collections import defaultdict
from pathlib import Path


METRICS = (
    'group_ticks_per_second', 'compute', 'computed_to_durable_confirmation',
    'send_to_master_receive', 'master_intake_to_commit_confirmation',
    'source_to_final_join_confirmation', 'worker_cpu_seconds', 'master_cpu_seconds',
    'worker_peak_aggregate_rss_bytes', 'master_peak_rss_bytes', 'outbox_backlog_max',
    'input_kafka_lag_records_max',
)


def spread(values):
    return {'n': len(values), 'mean': statistics.mean(values),
            'median': statistics.median(values), 'min': min(values), 'max': max(values),
            'sample_stdev': statistics.stdev(values) if len(values) > 1 else None}


def summarize(paths):
    grouped, excluded = defaultdict(list), []
    executions = set()
    for path in paths:
        run = json.loads(path.read_text(encoding='utf-8-sig'))
        measurement = run.get('measurements') or {}
        if run.get('status') != 'PASS' or run.get('comparison', {}).get('status') != 'PASS' or measurement.get('status') != 'PASS':
            excluded.append({'path': str(path), 'status': run.get('status'),
                             'reason': run.get('error', 'missing successful parity/measurement')})
            continue
        if run['execution_id'] in executions:
            raise ValueError('the same execution cannot count as independent repeats')
        executions.add(run['execution_id'])
        spec = dict(run['configuration'])
        spec.pop('name')
        environment = run['environment']
        identity = {'source_commit': environment['source_commit'], 'configuration': spec,
                    'profile': environment.get('profiling_enabled', False),
                    'run_id': run['run_id'], 'config_id': run['config_id'],
                    'go': environment['go'], 'worker_gomaxprocs': environment['worker_gomaxprocs'],
                    'master_gomaxprocs': environment['master_gomaxprocs'],
                    'postgres_settings': {key: value for key, value in environment.items() if key.startswith('postgres_')}}
        key = json.dumps(identity, sort_keys=True)
        grouped[key].append((path, run))
    groups = []
    for key, entries in sorted(grouped.items()):
        identity = json.loads(key)
        metrics = {}
        for metric in METRICS:
            values = [run['measurements'][metric] for _, run in entries]
            if isinstance(values[0], dict):
                metrics[metric] = {field: spread([v[field] for v in values])
                                   for field in ('mean_seconds', 'p50_seconds', 'p95_seconds', 'p99_seconds')}
            else:
                metrics[metric] = spread(values)
        groups.append({**identity, 'independent_repeats': len(entries),
                       'status': 'PROFILE_ONLY' if identity['profile'] else ('PRELIMINARY' if len(entries) < 3 else 'REPEATED'),
                       'metrics': metrics,
                       'artifacts': [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                                      'execution_id': run['execution_id'], 'binary_sha256': run['binary_sha256']}
                                     for path, run in entries]})
    for group in groups:
        reference = next((g for g in groups if g['configuration']['workers'] == 1 and
                          {k: v for k, v in g.items() if k in ('source_commit', 'run_id', 'config_id', 'profile', 'go')} ==
                          {k: v for k, v in group.items() if k in ('source_commit', 'run_id', 'config_id', 'profile', 'go')} and
                          {k: v for k, v in g['configuration'].items() if k != 'workers'} ==
                          {k: v for k, v in group['configuration'].items() if k != 'workers'}), None)
        if reference:
            speedup = group['metrics']['group_ticks_per_second']['median'] / reference['metrics']['group_ticks_per_second']['median']
            group['speedup_from_matching_one_worker_medians'] = speedup
            group['parallel_efficiency'] = speedup / group['configuration']['workers']
    return {'schema_version': 1, 'groups': groups, 'excluded': excluded,
            'limitations': ['Message quantiles are within-run distributions, not independent replications.',
                            'No confidence interval or universal transport/cadence ranking is inferred.',
                            'Profile and fault runs never enter clean throughput comparisons.',
                            'Inspect recorded environment and exact parameters before cross-commit comparison.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', action='append', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    with args.output.open('x', encoding='utf-8', newline='\n') as output:
        json.dump(summarize(args.run), output, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
