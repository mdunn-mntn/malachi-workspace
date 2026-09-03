#!/bin/zsh
# Usage: audi_1270_archive_scan.sh <out_tsv> <YYYYMMDDHH prefix>...
# Lists spark-events objects per hour prefix and records app name + size per object.
set -u
OUT=$1; shift
BUCKET=gs://mntn-data-archive-prod/spark-events
name_of() {
  obj=$1
  n=$(gsutil -o "GSUtil:check_hashes=never" cat -r 0-262143 "$obj" 2>/dev/null | /opt/homebrew/bin/zstd -d -c 2>/dev/null | grep -o '"App Name":"[^"]*"' | head -1 | sed 's/"App Name":"//;s/"$//')
  printf "%s\t%s\n" "$obj" "${n:-?}"
}
for h in "$@"; do
  gsutil ls -l "$BUCKET/app-$h*" 2>/dev/null | grep -v TOTAL | awk '{print $1"\t"$3}' | while IFS=$'\t' read -r size obj; do
    printf "%s\t%s\n" "$size" "$(name_of "$obj")" >> "$OUT"
  done
done
