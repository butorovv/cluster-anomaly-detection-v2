"""Separate startup, a declared warm-up interval, offered-load interior and drain.

Time boundaries are current processing clocks. Summit tick strings are used only
as join keys, never subtracted from wall time. Burst interiors are not evidence
of a sustained steady state; offered-rate experiments must establish that.
"""
import argparse
import json
import math
from datetime import datetime
from pathlib import Path


def seconds(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def quantiles(values):
    values = sorted(values)
    if not values:
        return {'count': 0}
    return {'count': len(values), 'mean_seconds': sum(values)/len(values),
            **{name: values[math.ceil(fraction*len(values))-1]
               for name, fraction in [('p50_seconds', .5), ('p95_seconds', .95), ('p99_seconds', .99)]}}


def analyze(path, warmup):
    run = json.loads(path.read_text(encoding='utf-8-sig'))
    if run['status'] != 'PASS' or run['comparison']['status'] != 'PASS' or run['configuration'].get('fault'):
        raise ValueError('phase analysis requires independent full-run parity')
    ticks, master, joins, first_commit = [], [], {}, {}
    for log in Path(run['log_directory']).glob('*.log'):
        for line in log.read_text(encoding='utf-8-sig').splitlines():
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if row.get('execution') != run['execution_id']:
                continue
            if row.get('msg') == 'tick measurement':
                ticks.append(row)
            if row.get('msg') == 'result measurement':
                master.append(row)
                if row['kind'] == 'tick_fragment':
                    key = row['message_id']
                    prior = first_commit.get(key)
                    if prior is None or seconds(row['committed_at']) < seconds(prior['committed_at']):
                        first_commit[key] = row
    if len(ticks) != run['comparison']['raw_rows']:
        raise ValueError('tick traces require replay-aware analysis')
    for row in first_commit.values():
        joins[row['tick']] = max(joins.get(row['tick'], 0), seconds(row['committed_at']))
    first = min(seconds(t['source_first_at']) for t in ticks)
    last_publication = max(seconds(t['source_marker_at']) for t in ticks)
    finish = max(joins.values())
    phases = {}
    for name, start, end in [('warmup', first, min(first+warmup, last_publication)),
                             ('offered_load_interior', min(first+warmup, last_publication), last_publication),
                             ('drain', last_publication, finish)]:
        compute = [t['compute_seconds'] for t in ticks if start <= seconds(t['computed_at']) < end]
        service = [seconds(t['committed_at'])-seconds(t['received_at']) for t in master
                   if start <= seconds(t['received_at']) < end]
        finalized = sum(start <= at and (at < end or (name == 'drain' and at == end))
                        for at in joins.values()) * run['configuration']['groups']
        phases[name] = {'start_processing_unix_seconds': start, 'end_processing_unix_seconds': end,
                        'seconds': max(0, end-start), 'computed': quantiles(compute),
                        'intake_to_commit': quantiles(service), 'final_group_ticks': finalized,
                        'final_group_ticks_per_second': finalized/(end-start) if end > start else None}
    return {'schema_version': 1, 'run': str(path), 'warmup_seconds_fixed_by_analysis_protocol': warmup,
            'startup_to_first_publication_seconds': first-seconds(run['started_at']),
            'post_pipeline_comparison_cleanup_seconds': seconds(run['finished_at'])-finish,
            'publication_record_clock_span_seconds': last_publication-first,
            'actual_group_ticks_publication_rate': len(ticks)/(last_publication-first),
            'actual_input_records_including_markers_per_second': len(ticks)*19/(last_publication-first),
            'publication_clock_caveat': 'Producer record timestamps, not broker ACK times; canonical D2 has 18 hosts plus one marker per group tick.',
            'steady_state_status': 'NOT_ESTABLISHED_BURST' if run['configuration']['replay_speed'] == 0 else 'PACED_INTERIOR_REQUIRES_STATIONARITY_CHECK',
            'phases': phases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--warmup-seconds', type=float, default=30)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.warmup_seconds <= 0:
        parser.error('positive warmup required')
    with args.output.open('x', encoding='utf-8', newline='\n') as output:
        json.dump(analyze(args.run, args.warmup_seconds), output, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
