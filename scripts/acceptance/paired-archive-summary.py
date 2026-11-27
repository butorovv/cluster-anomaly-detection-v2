import collections
import datetime as dt
import json
import math
import pathlib
import statistics
import sys


def get_size(raw):
    units = {"B": 1, "kB": 1000, "MB": 10 ** 6, "GB": 10 ** 9,
             "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3}
    for unit in sorted(units, key=len, reverse=True):
        if raw.endswith(unit):
            return float(raw[:-len(unit)]) * units[unit] / (1024 ** 2)
    raise ValueError(raw)


def snapshot(path):
    lines = path.read_text().splitlines()
    stats = [int(value) for value in lines[0].split("|")]
    disk = next(line.split() for line in lines[2:] if line.split()[2] == sys.argv[2])
    return stats, int(lines[1]), (int(disk[5]) * 512, int(disk[9]) * 512)


def rows(path):
    return [line.split("\t") for line in path.read_text().splitlines()]


def percentile(values, ratio):
    return sorted(values)[math.ceil(len(values)*ratio) - 1]


def instant(path):
    return dt.datetime.fromisoformat(path.read_text().strip().replace("Z", "+00:00"))


def stats(path):
    groups = collections.defaultdict(lambda: collections.defaultdict(lambda: [0, 0]))
    for line in path.read_text().splitlines():
        row = json.loads(line)
        name = row.get("Name", "")
        if name == sys.argv[3]:
            kind = "archive_pg"
        elif "-worker-" in name or "-master-" in name:
            kind = "diagnostics"
        elif "-postgres-" in name:
            kind = "diagnostic_pg"
        else:
            continue
        sample = groups[kind][row["sample"]]
        sample[0] += float(row["CPUPerc"].removesuffix("%"))
        sample[1] += get_size(row["MemUsage"].split(" / ")[0])
    return {key: [round(max(x[0] for x in value.values()), 2),
                  round(max(x[1] for x in value.values()), 2), len(value)]
            for key, value in groups.items()}


for directory in sorted(pathlib.Path(sys.argv[1]).glob("[1-3]-*")):
    if not (directory / "finalized.txt").is_file():
        continue
    report = json.loads((directory / "env/verification.json").read_text())
    interval = report["pipeline"]["wallSeconds"]
    before, before_wal, before_disk = snapshot(directory / "before.txt")
    after, after_wal, after_disk = snapshot(directory / "after.txt")
    out = dict(case=directory.name, diagnostic_s=round(interval, 4),
               tick_rate=round(report["groupTicks"] / interval, 2),
               e2e_s=round((instant(directory / "finalized.txt") - instant(directory / "begin.txt")).total_seconds(), 2),
               peak_samples=stats(directory / "docker-stats.jsonl"),
               pg_delta=[a-b for a, b in zip(after, before)], wal=after_wal-before_wal,
               host_disk=[a-b for a, b in zip(after_disk, before_disk)])
    if (directory / "archive.tsv").is_file():
        covered = rows(directory / "archive.tsv")
        diag = {(ts, part): float(observed) for ts, part, observed in rows(directory / "diagnostics.tsv")}
        differences = [1000 * (float(observed) - diag[(ts, part)]) for ts, part, observed, _, _ in covered]
        duration, user_cpu, sys_cpu, rss = map(float, (directory / "archive-time.txt").read_text().split())
        out.update(archive_s=duration, archive_cpu_s=round(user_cpu+sys_cpu, 2),
                   archive_rss_kib=int(rss), covered=len(covered),
                   inserted=sum(int(row[3]) for row in covered), duplicates=sum(int(row[4]) for row in covered),
                   signed_lag_ms=[round(statistics.median(differences), 1), round(percentile(differences, .95), 1),
                                  round(max(differences), 1)])
    print(json.dumps(out, ensure_ascii=False))
