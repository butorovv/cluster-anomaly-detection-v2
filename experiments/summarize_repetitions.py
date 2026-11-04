"""Summarize actual matched transport trials. Ranges are not confidence intervals."""
import argparse
import hashlib
import json
import statistics
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--matrix', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    runs = json.loads(args.matrix.read_text(encoding='utf-8-sig'))
    fields = ('workers', 'groups', 'delta', 'cadence', 'poll_ms', 'replay_speed')
    modes = ('http_push', 'polling', 'kafka')
    base = runs[0]
    if len(runs) != 6:
        raise ValueError('six complete two-trial controls required')
    trials = []
    for run in runs:
        cfg, m = run['configuration'], run['measurements']
        if run['status'] != 'PASS' or run['comparison']['status'] != 'PASS' or m['status'] != 'PASS':
            raise ValueError('failed run cannot enter comparative results')
        if any(cfg[k] != base['configuration'][k] for k in fields):
            raise ValueError('unmatched control parameters')
        for key in ('run_id', 'config_id', 'configuration_sha256', 'binary_sha256'):
            if run[key] != base[key]:
                raise ValueError(f'unmatched scientific/build/global configuration: {key}')
        if run['comparison']['canonical_sha256'] != base['comparison']['canonical_sha256']:
            raise ValueError('cross-trial scientific payload mismatch')
        counters = m['counters']
        if counters['output_attempt_payload_bytes'] != base['measurements']['counters']['output_attempt_payload_bytes']:
            raise ValueError('payload sizes differ')
        if m['input_kafka_lag_status'] != 'MEASURED_PARTITION_HIGH_WATERMARK_1HZ':
            raise ValueError('invalid legacy lag aggregate')
        trials.append({'scenario': cfg['name'], 'mode': cfg['mode'],
                       'throughput': m['group_ticks_per_second'],
                       'transport_p95_seconds': m['send_to_master_receive']['p95_seconds'],
                       'e2e_p95_seconds': m['source_to_final_join_confirmation']['p95_seconds'],
                       'compute_p95_seconds': m['compute']['p95_seconds'],
                       'durability_p95_seconds': m['computed_to_durable_confirmation']['p95_seconds'],
                       'worker_cpu_seconds': m['worker_cpu_seconds'], 'master_cpu_seconds': m['master_cpu_seconds'],
                       'worker_peak_rss_bytes': m['worker_peak_aggregate_rss_bytes'],
                       'master_peak_rss_bytes': m['master_peak_rss_bytes'],
                       'sampled_input_lag_max': m['input_kafka_lag_records_max'],
                       'sampled_outbox_backlog_max': m['outbox_backlog_max'],
                       'replay_seconds_including_startup': run['replay_seconds'],
                       'postgres_wait_samples': m.get('postgres_active_wait_samples', {}),
                       'postgres_sample_count': m['postgres_wait_sample_count'],
                       'retries': counters['output_retries'], 'duplicates': counters['master_duplicates'],
                       'kafka_stages': {key: {'count': h['count'], 'mean_seconds': h['sum_seconds']/h['count'],
                                              'max_seconds': h['max_seconds']}
                                        for key, h in m.get('kafka_stage_histograms', {}).items() if h['count']}})
    summary = {}
    for mode in modes:
        selected = [r for r in trials if r['mode'] == mode]
        if len(selected) != 2:
            raise ValueError('two trials per mode required')
        summary[mode] = {'trials': 2}
        for key in ('throughput', 'transport_p95_seconds', 'e2e_p95_seconds'):
            values = [r[key] for r in selected]
            summary[mode][key] = {'min': min(values), 'mean': statistics.mean(values), 'max': max(values)}
    result = {'schema_version': 1, 'status': 'PASS', 'source_matrix': str(args.matrix),
              'source_matrix_sha256': hashlib.sha256(args.matrix.read_bytes()).hexdigest(),
              'matched_configuration': {k: base['configuration'][k] for k in fields},
              'source_commit': base['environment']['source_commit'],
              'run_id': base['run_id'], 'config_id': base['config_id'],
              'canonical_payload_sha256': base['comparison']['canonical_sha256'],
              'trials': trials, 'summary': summary,
              'limitations': ['Two trials, one host; no robust confidence intervals.',
                              'One brief Python historical trace audit overlapped late drain of first Push trial; builds and integration suites finished before all controls.',
                              'Burst replay speed=0 is matched, actual source arrival rates vary under load; these are saturation/queue observations, not fixed offered-rate network latency.',
                              'PostgreSQL wait samples are backend observations, not exact durations.',
                              'CreateTime-to-poll combines producer transit, broker and client prefetch.',
                              'New one-Worker results must not be pooled with historical four-Worker trials.']}
    with args.output.open('x', encoding='utf-8', newline='\n') as f:
        json.dump(result, f, indent=2)
        f.write('\n')
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    for ax, key, ylabel in zip(axes, ('throughput', 'transport_p95_seconds', 'e2e_p95_seconds'),
                               ('Group ticks/s', 'Send to intake p95, seconds', 'Source to final join p95, seconds')):
        for x, mode in enumerate(modes):
            values = [r[key] for r in trials if r['mode'] == mode]
            ax.plot([x, x], [min(values), max(values)], color='tab:blue')
            ax.scatter([x-.05, x+.05], values, color='tab:blue')
            ax.scatter([x], [statistics.mean(values)], marker='D', color='black', s=20)
        ax.set_xticks(range(3), ('Push', 'Polling 100ms', 'Kafka'))
        ax.set_ylabel(ylabel)
        ax.grid(alpha=.3)
        if key == 'transport_p95_seconds':
            ax.set_yscale('log')
    fig.suptitle('Full D2 / one Worker / cadence 3: two observed trials; ranges are not CIs')
    fig.tight_layout()
    fig.savefig(args.output.with_suffix('.png'), dpi=180)
    plt.close(fig)


if __name__ == '__main__':
    main()
