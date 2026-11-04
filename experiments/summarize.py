"""Summarize observed runs only. No detector or synthetic measurements."""
import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--matrix', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    runs = []
    for path in args.matrix:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
        runs.extend(data if isinstance(data, list) else [data])
    parity = []
    measured = []
    for run in runs:
        cfg, comparison = run['configuration'], run['comparison']
        parity.append({**cfg, 'status': run['status'], 'comparison_status': comparison.get('status'),
                       **{k: comparison.get(k) for k in ('raw_rows', 'temporal_rows', 'evidence_rows', 'positive_ticks', 'parent_events', 'new_events', 'rb_subspans', 'rb_supported_parents')},
                       'execution_id': run['execution_id'], 'error': run.get('error', '')})
        m = run.get('measurements')
        if run['status'] != 'PASS' or not m or m['status'] != 'PASS' or cfg.get('fault'):
            continue
        row = {**cfg, 'ticks_per_second': m['group_ticks_per_second'],
               'worker_cpu_seconds': m['worker_cpu_seconds'], 'master_cpu_seconds': m['master_cpu_seconds'],
               'worker_peak_rss_bytes': m['worker_peak_aggregate_rss_bytes'], 'master_peak_rss_bytes': m['master_peak_rss_bytes'],
               'outbox_backlog_max': m['outbox_backlog_max'],
               'kafka_lag_status': m.get('input_kafka_lag_status', 'NOT_VERIFIED_LEGACY_SAMPLER'),
               'kafka_lag_records_max': m['input_kafka_lag_records_max'] if m.get('input_kafka_lag_status', '').startswith('MEASURED_') else None,
               'resource_samples': m['resource_samples']}
        for key in ('compute', 'computed_to_durable_confirmation', 'send_to_master_receive', 'master_intake_to_commit_confirmation', 'source_to_final_join_confirmation'):
            for q in ('count', 'p50_seconds', 'p95_seconds', 'p99_seconds'):
                row[f'{key}_{q}'] = m[key][q]
        row.update(m['counters'])
        measured.append(row)
    def write(name, rows):
        if not rows:
            return
        columns = list(dict.fromkeys(k for row in rows for k in row))
        with (args.output / name).open('w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns, lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
    # Speedup uses ONLY the matched observed N=1 baseline.
    for row in measured:
        baseline = next((r for r in measured if r['workers'] == 1 and all(r[k] == row[k] for k in ('groups', 'delta', 'mode', 'cadence', 'poll_ms', 'replay_speed'))), None)
        if baseline:
            row['speedup'] = row['ticks_per_second'] / baseline['ticks_per_second']
            row['efficiency'] = row['speedup'] / row['workers']
    write('parity.csv', parity)
    write('measurements.csv', measured)
    if not measured:
        return
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.size': 10})
    scaling = sorted((r for r in measured if r['name'].startswith('scaling-')), key=lambda r: r['workers'])
    if scaling:
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        axes[0].plot([r['workers'] for r in scaling], [r['ticks_per_second'] for r in scaling], 'o-')
        axes[0].set(xlabel='Worker processes (GOMAXPROCS=1)', ylabel='Group ticks / second', title='Observed full-D2 throughput')
        axes[1].plot([r['workers'] for r in scaling], [r.get('speedup', float('nan')) for r in scaling], 'o-', label='Observed')
        axes[1].plot([r['workers'] for r in scaling], [r['workers'] for r in scaling], '--', label='Ideal reference')
        axes[1].set(xlabel='Worker processes', ylabel='Speedup'); axes[1].legend()
        for ax in axes: ax.grid(alpha=.3)
        fig.tight_layout(); fig.savefig(args.output / 'scaling.png', dpi=180); plt.close(fig)
    if scaling:
        fig, axes = plt.subplots(1, 2, figsize=(10, 4))
        for field, label in (('worker_cpu_seconds', 'Workers sum'), ('master_cpu_seconds', 'Master')):
            axes[0].plot([r['workers'] for r in scaling], [r[field] for r in scaling], 'o-', label=label)
        for field, label in (('worker_peak_rss_bytes', 'Workers aggregate sampled peak'), ('master_peak_rss_bytes', 'Master sampled peak')):
            axes[1].plot([r['workers'] for r in scaling], [r[field]/(1024**2) for r in scaling], 'o-', label=label)
        axes[0].set(xlabel='Worker processes', ylabel='Cumulative process CPU seconds', title='Runtime CPU (includes startup)')
        axes[1].set(xlabel='Worker processes', ylabel='MiB RSS', title='Observed memory / 1 second sampling')
        for ax in axes: ax.grid(alpha=.3); ax.legend(fontsize=8)
        fig.tight_layout(); fig.savefig(args.output / 'resources.png', dpi=180); plt.close(fig)
    transports = [r for r in measured if r['groups'] == 16 and r['delta'] == 0 and r['workers'] == 4 and r['cadence'] == 3]
    if transports:
        labels = [r['mode'] + (f" {r['poll_ms']}ms" if r['mode'] == 'polling' else '') for r in transports]
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
        for ax, component, title in ((axes[0], 'send_to_master_receive', 'Send → Master intake (log scale)'), (axes[1], 'source_to_final_join_confirmation', 'Source CreateTime → final join ACK')):
            x = list(range(len(transports)))
            for offset, q in ((-.22, 'p50_seconds'), (0, 'p95_seconds'), (.22, 'p99_seconds')):
                ax.bar([i+offset for i in x], [r[f'{component}_{q}'] for r in transports], width=.22, label=q[:3])
            ax.set_xticks(x, labels, rotation=20, ha='right'); ax.set(ylabel='Seconds', title=title); ax.legend()
            if component == 'send_to_master_receive': ax.set_yscale('log')
        fig.tight_layout(); fig.savefig(args.output / 'transport-latency.png', dpi=180); plt.close(fig)
    recoveries = [r for r in runs if r['status']=='PASS' and r.get('fault', {}).get('kind')=='kill_worker' and r['configuration']['name'].startswith('crash-')]
    if recoveries:
        fig, ax = plt.subplots(figsize=(7,4))
        ax.bar([r['configuration']['name'] for r in recoveries], [r['fault']['time_to_resumed_checkpoint_seconds'] for r in recoveries])
        ax.set(ylabel='Seconds', title='Kill → resumed checkpoint (cadence 3/6)');ax.tick_params(axis='x',rotation=25)
        fig.tight_layout();fig.savefig(args.output/'recovery.png',dpi=180);plt.close(fig)

if __name__ == '__main__':
    main()
