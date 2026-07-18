#!/usr/bin/env bash
# Introspect a BigQuery dataset -> per-table catalog docs under knowledge/bq/<dataset>/.
# Regenerates the AUTO:SCHEMA block + derived front-matter; preserves human-written sections.
# Usage: scripts/bq_introspect.sh <dataset>
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

DATASET="${1:-}"
if [ -z "$DATASET" ]; then
  echo "usage: scripts/bq_introspect.sh <dataset>   (project=$GCP_PROJECT)" >&2
  exit 1
fi
: "${GCP_PROJECT:?set GCP_PROJECT in scripts/config.env}"
: "${BQ_REGION:?set BQ_REGION (e.g. region-us-central1) in scripts/config.env}"
command -v bq   >/dev/null || { echo "bq not found (install google-cloud-sdk)" >&2; exit 1; }
command -v jq   >/dev/null || { echo "jq not found" >&2; exit 1; }

# Pin the job location to the configured region so we don't depend on ~/.bigqueryrc defaults.
LOCATION="${BQ_REGION#region-}"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
bqq() { bq query --project_id="$GCP_PROJECT" --location="$LOCATION" --use_legacy_sql=false \
                 --format=prettyjson --max_rows=100000 "$1"; }

INFO="\`${GCP_PROJECT}.${DATASET}.INFORMATION_SCHEMA"
echo "Introspecting ${GCP_PROJECT}.${DATASET} (location=$LOCATION) ..."

bqq "SELECT table_name, column_name, ordinal_position, data_type, is_nullable,
            is_partitioning_column, clustering_ordinal_position
     FROM ${INFO}.COLUMNS\`
     ORDER BY table_name, ordinal_position"                         > "$TMP/columns.json"

# TABLE_STORAGE is NOT a dataset-scoped view — it only exists at region/project scope.
# Query it region-scoped and filter to this dataset. (Views return no storage rows; that's fine.)
bqq "SELECT table_name, total_rows, total_logical_bytes
     FROM \`${GCP_PROJECT}.${BQ_REGION}.INFORMATION_SCHEMA.TABLE_STORAGE\`
     WHERE table_schema = '${DATASET}'"                            > "$TMP/storage.json"

bqq "SELECT table_name, table_type
     FROM ${INFO}.TABLES\`
     WHERE table_type IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW')
     ORDER BY table_name"                                           > "$TMP/tables.json"

# Logical-view SQL definitions (empty array if the dataset has none).
bqq "SELECT table_name, view_definition
     FROM ${INFO}.VIEWS\`" > "$TMP/views.json" 2>/dev/null || echo "[]" > "$TMP/views.json"

python3 "$HERE/_render_table_doc.py" "$DATASET" "$ROOT/knowledge/bq/$DATASET" \
        "$TMP/columns.json" "$TMP/storage.json" "$TMP/tables.json" "$TMP/views.json"

echo "Done. Enrich the human sections, then run: $HERE/build_index.sh"
