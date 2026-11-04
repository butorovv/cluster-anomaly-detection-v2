"""Inspect the actual approved Parquet with an independent Arrow reader."""
import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


def sha(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--assets', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    path = args.assets / 'data/prepared/summit-D1-pilot/D1.parquet'
    manifest = path.with_name('manifest.json')
    file_sha = sha(path)
    if file_sha != 'cf3fa634448b7147209bebf3b1c20a1a031be5c047678083adda10581ff562e7':
        raise ValueError('unapproved prepared input')
    start = datetime(2022, 1, 18, 8, tzinfo=timezone.utc)
    end = datetime(2022, 1, 18, 16, tzinfo=timezone.utc)
    total = 0
    first, last, selected_first, selected_last, previous = None, None, None, None, None
    ticks, groups = Counter(), Counter()
    source = pq.ParquetFile(path)
    for batch in source.iter_batches(batch_size=32768, columns=['timestamp', 'hostname', 'group_id']):
        for row in batch.to_pylist():
            ts, host, group = row['timestamp'], row['hostname'], row['group_id']
            key = (ts, host)
            if previous is not None and key <= previous:
                raise ValueError('non-increasing actual Parquet input')
            previous = key
            first = ts if first is None else first
            last = ts
            total += 1
            if start <= ts < end:
                selected_first = ts if selected_first is None else selected_first
                selected_last = ts
                ticks[(ts, group)] += 1
                groups[group] += 1
    if total != 2488320 or sum(groups.values()) != 829440 or len(ticks) != 46080:
        raise ValueError('actual D2 membership/count mismatch')
    if any(n != 18 for n in ticks.values()) or len(groups) != 16:
        raise ValueError('unexpected real D2 missingness/universe')
    result = {'schema_version': 1, 'status': 'PASS', 'reader': 'independent pyarrow ParquetFile',
              'input_path': str(path), 'input_sha256': file_sha, 'manifest_sha256': sha(manifest),
              'profile_sha256': sha(args.assets/'configs/group-profile.summit-v1.json'),
              'actual_file_rows': total, 'actual_file_first': first.isoformat(), 'actual_file_last': last.isoformat(),
              'window_start_inclusive': start.isoformat(), 'window_end_exclusive': end.isoformat(),
              'selected_first': selected_first.isoformat(), 'selected_last': selected_last.isoformat(),
              'selected_records': sum(groups.values()), 'group_ticks': len(ticks),
              'expected_full_grid_completion_markers': len(ticks), 'records_per_group': dict(groups),
              'rows_per_group_tick': 18, 'strict_order_checked': True,
              'explanation': 'D1-pilot is the prepared full-day asset name (00:00–23:59:50); D2 is its immutable 08:00 inclusive–16:00 exclusive evaluation window. No D1 training window is replayed.',
              'wire_exact_float_bit_evidence': 'P13 TestRealAcceptance/TestRealD2InputPipeline: 829440 records and 46080 assembled markers; no invalid/partial/empty real D2 ticks',
              'raw_source_sha_status': 'declared by immutable manifest, original raw source not rehashed in this audit'}
    with args.output.open('x', encoding='utf-8') as f:
        json.dump(result, f, indent=2)
        f.write('\n')


if __name__ == '__main__':
    main()
