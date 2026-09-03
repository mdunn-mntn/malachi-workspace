"""Match Dataproc batches to spark-events objects by start time and emit a download manifest.

Usage: python3 audi_1271_match_logs.py <batches_csv> <listing_csv> <since_iso_date> <out_manifest_csv>
"""

from __future__ import annotations

import csv
import re
import sys
from datetime import datetime, timedelta, timezone

MIN_BYTES = 1 << 20
WINDOW = timedelta(minutes=6)
STAMP = re.compile(r"app-(\d{17})-\d+\.zstd$")


def parse_batches(path: str, since: datetime) -> list[tuple[str, datetime, int]]:
    rows = []
    with open(path) as f:
        for name, created, dcu in csv.reader(f):
            ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if ts >= since:
                rows.append((name, ts, int(dcu or 0)))
    return sorted(rows, key=lambda r: r[1])


def parse_listing(path: str) -> list[tuple[datetime, int, str]]:
    objs = []
    with open(path) as f:
        for row in csv.reader(f):
            if len(row) != 3 or not row[2].startswith("gs://"):
                continue
            size, _, uri = row
            m = STAMP.search(uri)
            if not m or int(size) < MIN_BYTES:
                continue
            ts = datetime.strptime(m.group(1)[:14], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            objs.append((ts, int(size), uri))
    return sorted(objs)


def main(batches_csv: str, listing_csv: str, since_iso: str, out_csv: str) -> None:
    since = datetime.fromisoformat(since_iso).replace(tzinfo=timezone.utc)
    batches = parse_batches(batches_csv, since)
    objs = parse_listing(listing_csv)
    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["batch", "batch_create_utc", "milli_dcu_seconds", "app_start_utc", "size_bytes", "uri"])
        for name, ts, dcu in batches:
            hits = [o for o in objs if ts <= o[0] <= ts + WINDOW]
            for ots, size, uri in hits:
                w.writerow([name, ts.isoformat(), dcu, ots.isoformat(), size, uri])
            if not hits:
                w.writerow([name, ts.isoformat(), dcu, "", "", ""])
    print(f"{len(batches)} batches since {since_iso}; manifest -> {out_csv}")


if __name__ == "__main__":
    main(*sys.argv[1:5])
