#!/usr/bin/env bash
# AUDI-1170 — household feature-store backfill (dev + copy-to-prod, Ryan's approach)
#
# Runs the 3 household models in DEV via model_run.py, then copies the new
# partitions to PROD. Dependency order is enforced: mirror -> L2 -> L3.
#   L1 mirror : identity_graph_ip_household_id        (~13 weekly graph dates)
#   L2 derived: guid_log_derived_household_id_vertical_id   (90 daily dates)
#   L3 pivot  : guid_log_pivot_household_id_vertical_id     (90 daily dates)
# Range: 2026-05-08 .. 2026-08-05 (graph floor is ~2026-04-20, so 90d is clean).
#
# USAGE (always DRY_RUN by default — it only prints commands):
#   bash backfill_household_fs.sh smoke        # 1 old date end-to-end (mirror->L2->L3), then inspect
#   DRY_RUN=0 bash backfill_household_fs.sh seed      # prod L1 guid_log -> dev (L2's 30d-lookback input)
#   DRY_RUN=0 bash backfill_household_fs.sh smoke
#   DRY_RUN=0 bash backfill_household_fs.sh mirror       # ~13 weekly mirror runs, newest first, parallel
#   DRY_RUN=0 bash backfill_household_fs.sh copy-mirror  # mirror dev -> PROD (MUST run before daily:
#                                                        #   L2 reads the mirror from PROD even in dev)
#   DRY_RUN=0 bash backfill_household_fs.sh daily        # 90x (L2 -> L3) date-pairs, oldest first, parallel
#   DRY_RUN=0 bash backfill_household_fs.sh copy         # L2+L3 dev -> prod (writes PROD)
# CONCURRENCY=4 by default (parallel Dataproc batches; Ryan OK'd running these simultaneously).
# Read-resolution (compiled model_config.json): guid_log L1 reads DEV (hence seed); the graph mirror is
# read-only -> reads PROD always; L2/L3 read DEV. So: seed -> mirror -> copy-mirror -> daily -> copy.
#
# PREREQS: local airflow-ti on `main` (has the merged household models);
#   `gcloud auth login` + `gcloud auth application-default login`; `uv sync --group models`.
set -uo pipefail

# ---------- config ----------
AIRFLOW_TI="${AIRFLOW_TI:-$HOME/Developer/work/mntn/airflow-ti}"
START="${START:-2026-05-08}"; END="${END:-2026-08-05}"
SMOKE_DATE="${SMOKE_DATE:-2026-05-15}"   # old enough that PROD mirror lacks that week -> a real test
DRY_RUN="${DRY_RUN:-1}"                   # 1 = print only; 0 = actually run
SKIP_EXISTING="${SKIP_EXISTING:-1}"       # 1 = skip a date whose dev partition already exists (resume)
CONCURRENCY="${CONCURRENCY:-4}"           # parallel Dataproc batches for mirror/daily
MODE="${1:-smoke}"
ARG_DATE="${2:-}"

MIRROR="identity_graph_ip_household_id"
L2="guid_log_derived_household_id_vertical_id"
L3="guid_log_pivot_household_id_vertical_id"
DEV="gs://mntn-data-archive-dev/feature_store"
PROD="gs://mntn-data-archive-prod/feature_store"
REL_MIRROR="feature_group_1_source/$MIRROR"
REL_L2="feature_group_2_derived/$L2"
REL_L3="feature_group_3_pivoted/$L3"

# ---------- date helpers (macOS `date`) ----------
add_days() { date -j -v+"$2"d -f %Y-%m-%d "$1" +%Y-%m-%d; }   # add_days DATE N
seq_dates() { local d="$1"; while [[ "$d" < "$END" || "$d" == "$END" ]]; do echo "$d"; d=$(add_days "$d" "$2"); done; }

# ---------- run one model for one date (in dev, blocking) ----------
run_model() {
  local m="$1" d="$2" rel="$3" out_dt="$4"
  if [[ "$SKIP_EXISTING" == "1" && -n "$out_dt" ]] && gsutil -q stat "$DEV/$rel/dt=$out_dt/_SUCCESS" 2>/dev/null; then
    echo "  skip  [$m] $d (dev dt=$out_dt exists)"; return 0
  fi
  echo "  run   [$m] run_date=$d"
  if [[ "$DRY_RUN" == "0" ]]; then
    ( cd "$AIRFLOW_TI" && uv run python model_run.py "$m" -a "{\"run_date\": \"$d\"}" ) \
      || { echo "  FAIL  [$m] $d — stopping"; exit 1; }
  else
    echo "    DRY: (cd $AIRFLOW_TI && uv run python model_run.py $m -a '{\"run_date\": \"$d\"}')"
  fi
}

# ---------- copy a model's dev partitions -> prod (additive; never -d) ----------
copy_model() {
  local m="$1" rel="$2"
  echo ">> COPY $m  dev -> PROD"
  gsutil ls "$DEV/$rel/" 2>/dev/null | grep -E "/dt=[0-9-]+/$" | while read -r p; do
    local dt; dt=$(basename "$p")
    if [[ "$DRY_RUN" == "0" ]]; then gcloud storage cp -r -q "$DEV/$rel/$dt" "$PROD/$rel/";
    else echo "    DRY: gcloud storage cp -r $DEV/$rel/$dt $PROD/$rel/"; fi
  done
}

echo "MODE=$MODE  DRY_RUN=$DRY_RUN  range=$START..$END  airflow-ti=$AIRFLOW_TI"
[[ "$DRY_RUN" == "0" ]] && echo "!! LIVE RUN — this submits real Dataproc jobs (dev) / writes PROD (copy)."

case "$MODE" in
  seed)
    # L2 reads guid_log_ip_advertiser_id (L1) with a 30-day lookback, and dev reads resolve to the
    # DEV bucket only. Seed dev with the prod L1 partitions the backfill needs: START-30d .. END.
    REL_GUID="feature_group_1_source/guid_log_ip_advertiser_id"
    d=$(date -j -v-30d -f %Y-%m-%d "$START" +%Y-%m-%d)
    echo "== SEED dev L1 guid_log from prod: $d .. $END (~120 days, server-side copy) =="
    while [[ "$d" < "$END" || "$d" == "$END" ]]; do
      if gsutil -q stat "$DEV/$REL_GUID/dt=$d/_SUCCESS" 2>/dev/null; then echo "  skip  dt=$d (dev exists)"
      elif ! gsutil -q stat "$PROD/$REL_GUID/dt=$d/_SUCCESS" 2>/dev/null; then echo "  MISS  dt=$d (not in prod!)"
      elif [[ "$DRY_RUN" == "0" ]]; then echo "  copy  dt=$d"; gcloud storage cp -r -q "$PROD/$REL_GUID/dt=$d" "$DEV/$REL_GUID/" || { echo "  FAIL copy dt=$d — stopping"; exit 1; }
      else echo "    DRY: gcloud storage cp -r $PROD/$REL_GUID/dt=$d $DEV/$REL_GUID/"; fi
      d=$(add_days "$d" 1)
    done
    ;;
  smoke)
    echo "== SMOKE: mirror -> L2 -> L3 for $SMOKE_DATE (dev only) =="
    run_model "$MIRROR" "$SMOKE_DATE" "$REL_MIRROR" ""              # mirror dt = graph asOfDate (unknown here) -> no skip
    run_model "$L2" "$SMOKE_DATE" "$REL_L2" "$(add_days "$SMOKE_DATE" 1)"
    run_model "$L3" "$SMOKE_DATE" "$REL_L3" "$(add_days "$SMOKE_DATE" 1)"
    echo "== inspect dev output before the full run: =="
    echo "   gsutil ls $DEV/$REL_MIRROR/ ; gsutil ls $DEV/$REL_L2/ ; gsutil ls $DEV/$REL_L3/"
    echo "   If L2/L3 produced dt=$(add_days "$SMOKE_DATE" 1) with rows -> run: mirror, copy-mirror, daily, copy."
    echo "   NOTE the mirror read resolves to PROD (read-only model) -> copy-mirror must run before daily."
    ;;
  mirror)  echo "== MIRROR backfill (~weekly, newest first, x$CONCURRENCY) =="
           seq_dates "$START" 7 | tail -r | xargs -P "$CONCURRENCY" -I{} bash "$0" one-mirror {} ;;
  daily)   echo "== DAILY backfill (L2->L3 pairs, oldest first, x$CONCURRENCY) =="
           seq_dates "$START" 1 | xargs -P "$CONCURRENCY" -I{} bash "$0" pair {} ;;
  one-mirror) run_model "$MIRROR" "$ARG_DATE" "$REL_MIRROR" "" ;;
  pair)    run_model "$L2" "$ARG_DATE" "$REL_L2" "$(add_days "$ARG_DATE" 1)"
           run_model "$L3" "$ARG_DATE" "$REL_L3" "$(add_days "$ARG_DATE" 1)" ;;
  l2)      echo "== L2 backfill (daily) =="; for d in $(seq_dates "$START" 1); do run_model "$L2" "$d" "$REL_L2" "$(add_days "$d" 1)"; done ;;
  l3)      echo "== L3 backfill (daily) =="; for d in $(seq_dates "$START" 1); do run_model "$L3" "$d" "$REL_L3" "$(add_days "$d" 1)"; done ;;
  copy-mirror) copy_model "$MIRROR" "$REL_MIRROR" ;;
  copy)    copy_model "$L2" "$REL_L2"; copy_model "$L3" "$REL_L3" ;;
  *) echo "unknown MODE '$MODE' (use: seed|smoke|mirror|copy-mirror|daily|pair|l2|l3|copy)"; exit 2 ;;
esac
echo "done ($MODE)."
