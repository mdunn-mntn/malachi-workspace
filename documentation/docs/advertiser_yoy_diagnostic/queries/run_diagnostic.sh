#!/usr/bin/env bash
# Run the full advertiser YoY-decline diagnostic. Usage:
#   bash run_diagnostic.sh <AID> <WIN_START> <WIN_END> <P1_START> <P1_END> <P2_START> <P2_END> [OUTDIR]
# Dates: YYYY-MM-DD. WIN_* = full analysis window (covers both periods + context, e.g. 13-18 months).
set -uo pipefail
AID="$1"; WS="$2"; WE="$3"; P1S="$4"; P1E="$5"; P2S="$6"; P2E="$7"; OUT="${8:-./diag_out_$AID}"
mkdir -p "$OUT"; DIR="$(cd "$(dirname "$0")" && pwd)"
sub(){ sed -e "s/{{AID}}/$AID/g" -e "s/{{WIN_START}}/$WS/g" -e "s/{{WIN_END}}/$WE/g" \
           -e "s/{{P1_START}}/$P1S/g" -e "s/{{P1_END}}/$P1E/g" -e "s/{{P2_START}}/$P2S/g" -e "s/{{P2_END}}/$P2E/g" "$1"; }
for q in "$DIR"/*.sql; do
  n="$(basename "$q" .sql)"; echo "=== $n ==="
  bash /Users/malachi/Developer/work/mntn/workspace/.claude/scripts/bq_run.sh --ticket "AUDI-1070" --label "diag $AID $n" \
    --use_legacy_sql=false --format=csv --max_rows=5000 --project_id=dw-main-silver "$(sub "$q")" 2>/dev/null | tee "$OUT/$n.csv"
done
echo "Done -> $OUT"
